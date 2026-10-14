package com.db.rds.platform.prds.sourcing.day0.service;

import com.db.rds.platform.common.models.models.Node;                       // TODO: adjust import
import com.db.rds.platform.common.models.models.PrimaryIdentifier;
import com.db.rds.platform.prds.sourcing.day0.model.MigrationProduct;       // TODO: adjust import
import com.db.rds.platform.prds.sourcing.day0.model.NodeLevel;              // TODO: adjust import
import com.db.rds.platform.prds.sourcing.day0.model.ProductHierarchyData;   // TODO: adjust import
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.springframework.stereotype.Service;

// TODO: import the SAME constants ingestion uses when it creates a node (createHierarchyNodes)
import static com.db.rds.platform.prds.sourcing.day0.service.MigrationProductHierarchyService.ACTIVE_STATUS;
import static com.db.rds.platform.prds.sourcing.day0.service.MigrationProductHierarchyService.MIGRATION_PRODUCT_TYPE;

import java.math.BigDecimal;
import java.time.temporal.TemporalAccessor;
import java.util.*;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * One service, two jobs. No Oracle id is stored or used as a key anywhere.
 *
 *  1. findExistingIdentifier(...) - ingestion helper: reuse the node already stored for
 *                                   hierarchyName + nodeName + hierarchyPath instead of creating a duplicate.
 *  2. reconcile(...)              - read-only on-prem vs target comparison, ONE PRODUCT = ONE COMPLETE HIERARCHY:
 *
 *          productName  ->  full hierarchy path of that product (root/child/.../leaf)
 *
 *     Both sides are reduced to that same map and compared product by product.
 *
 *     On top of the path comparison, every other field is compared for products found on both sides:
 *       - product fields (type, code, description, long name, dates, ...)   -> PRODUCT_FIELD_MISMATCH
 *       - node fields along the hierarchy (description, level, status, ...) -> NODE_FIELD_MISMATCH
 *     Node fields are only compared when the two paths are identical, otherwise the nodes cannot be paired up.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class HierarchyNodeService {

    private final MigrationNodePrimeDataService primeData;          // nodes      TODO: adjust type/import
    private final MigrationProductPrimeDataService productData;     // products   TODO: adjust type/import

    // =====================================================================================
    // Result types
    // =====================================================================================

    public enum DiffType {
        MISSING_PRODUCT,              // product exists on-prem, not in the target
        EXTRA_PRODUCT,                // product exists in the target, not on-prem
        DUPLICATE_PRODUCT,            // same product name stored more than once in the target  <- re-run bug
        PATH_MISMATCH,                // same product, different complete hierarchy path
        MULTIPLE_HIERARCHIES,         // target product points at hierarchies that are not one single chain
        INCOMPLETE_HIERARCHY,         // a node in the product's parent chain has no name, path cannot be completed
        CYCLE,                        // a node in the product's parent chain is its own ancestor
        TARGET_PRODUCT_NAME_MISSING,
        SOURCE_PRODUCT_NAME_MISSING,
        SOURCE_NODE_NAME_MISSING,
        SOURCE_DUPLICATE_PRODUCT,     // same on-prem product name with two different paths
        PRODUCT_FIELD_MISMATCH,       // same product, a non-path field differs (type, code, description, ...)
        NODE_FIELD_MISMATCH           // same hierarchy path, a field of one of its nodes differs
    }

    public record ReconDifference(DiffType type, String key, String field,
                                  String sourceValue, String targetValue) {}

    public record ReconReport(String hierarchyName, int sourceProductCount, int targetProductCount,
                              Map<DiffType, Long> countsByType, List<ReconDifference> differences) {
        public boolean passed() { return differences.isEmpty(); }
    }

    private record ProductField(String name, Function<ProductHierarchyData, Object> source,
                                Function<MigrationProduct, Object> target) {}

    private record NodeField(String name, Object source, Object target) {}

    /**
     * Product fields compared between the on-prem row and the stored product (productName is the key, the hierarchy
     * path is compared separately). TODO: the SOURCE getters are assumed to share the target's names; adjust any
     * that differ on ProductHierarchyData, and delete a line if the on-prem row has no such field.
     * Not compared: dataOwner and recertificationDetails (no known on-prem equivalent).
     */
    private static final List<ProductField> PRODUCT_FIELDS = List.of(
            new ProductField("taxonomyUid", p -> p.getTaxonomyUid(), p -> p.getTaxonomyUid()),
            new ProductField("productType", p -> p.getProductType(), p -> p.getProductType()),
            new ProductField("code", p -> p.getCode(), p -> p.getCode()),
            new ProductField("migrationProductStatus", p -> p.getMigrationProductStatus(), p -> p.getMigrationProductStatus()),
            new ProductField("productDescription", p -> p.getProductDescription(), p -> p.getProductDescription()),
            new ProductField("longName", p -> p.getLongName(), p -> p.getLongName()),
            new ProductField("activeDate", p -> p.getActiveDate(), p -> p.getActiveDate()),
            new ProductField("lastChangedDateTime", p -> p.getLastChangedDateTime(), p -> p.getLastChangedDateTime()));

    // =====================================================================================
    // 1. Ingestion helper: find the node that already exists for this name + path
    // =====================================================================================

    public PrimaryIdentifier findExistingIdentifier(String hierarchyName, String nodeName, String nodePath) {
        if (hierarchyName == null || nodeName == null || nodePath == null) {
            log.warn("Existing-node lookup skipped, null field: {}/{}/{} - a new node will be created",
                    hierarchyName, nodeName, nodePath);
            return null;
        }

        List<Node> matches;
        try {
            matches = primeData.findByQuery(new Document("data.hierarchyName", hierarchyName)
                            .append("data.nodeName", nodeName)
                            .append("data.hierarchyPath", nodePath)
                            .toJson())
                    .collect(Collectors.toList());
        } catch (Exception e) {
            // never swallow: a failing query must not silently turn into "create a new node"
            throw new IllegalStateException(
                    "Existing-node lookup failed for " + hierarchyName + "/" + nodeName + "/" + nodePath, e);
        }

        if (matches.isEmpty()) {
            return null;
        }
        if (matches.size() > 1) {
            log.warn("{} nodes already stored for {}/{}/{} - reusing the first; run recon to clean up",
                    matches.size(), hierarchyName, nodeName, nodePath);
        }
        return matches.get(0).getIdentifier();
    }

    /** Same identity rule, usable as the in-run cache key in ingestion. */
    public static String naturalKey(String hierarchyName, String nodeName, String nodePath) {
        return nz(hierarchyName) + "|" + nz(nodeName) + "|" + nz(nodePath);
    }

    // =====================================================================================
    // 2. Reconciliation: productName -> hierarchy path, on both sides
    // =====================================================================================

    public ReconReport reconcile(String hierarchyName, List<ProductHierarchyData> sourceProducts) {
        List<ReconDifference> diffs = new ArrayList<>();

        Map<String, ProductHierarchyData> sourceByName = new LinkedHashMap<>();
        Map<String, MigrationProduct> targetByName = new LinkedHashMap<>();

        Map<String, String> source = buildSource(sourceProducts, diffs, sourceByName);

        // Products without a hierarchy carry no hierarchy name, so a hierarchy-based query can never find them
        // in the target. Look those up by product name instead.
        Set<String> noHierarchyProducts = source.entrySet().stream()
                .filter(e -> e.getValue().isEmpty()).map(Map.Entry::getKey)
                .collect(Collectors.toCollection(LinkedHashSet::new));

        Map<String, String> target = loadTarget(hierarchyName,
                sourceHierarchyNames(hierarchyName, sourceProducts), noHierarchyProducts, diffs, targetByName);

        // a product whose parent chain is broken has an unreliable path: it is already reported as
        // INCOMPLETE_HIERARCHY / CYCLE, so do not add a misleading PATH_MISMATCH on top
        Set<String> brokenChains = diffs.stream()
                .filter(d -> d.type() == DiffType.INCOMPLETE_HIERARCHY || d.type() == DiffType.CYCLE)
                .map(ReconDifference::key).collect(Collectors.toSet());

        Set<String> reportedNodeDiffs = new HashSet<>();
        for (Map.Entry<String, String> e : source.entrySet()) {
            String product = e.getKey();
            if (!target.containsKey(product)) {
                diffs.add(new ReconDifference(DiffType.MISSING_PRODUCT, product, "product", e.getValue(), null));
                continue;
            }
            String targetPath = target.get(product);
            boolean brokenChain = brokenChains.contains(product);
            boolean samePath = Objects.equals(e.getValue(), targetPath);
            if (!brokenChain && !samePath) {
                diffs.add(new ReconDifference(DiffType.PATH_MISMATCH, product, "hierarchyPath", e.getValue(), targetPath));
            }

            // everything except name + path: product fields always, node fields only when the nodes can be paired up
            compareProductFields(product, sourceByName.get(product), targetByName.get(product), diffs);
            if (!brokenChain && samePath) {
                compareNodeFields(sourceByName.get(product), targetByName.get(product), diffs, reportedNodeDiffs);
            }
        }
        for (Map.Entry<String, String> e : target.entrySet()) {
            if (!source.containsKey(e.getKey())) {
                diffs.add(new ReconDifference(DiffType.EXTRA_PRODUCT, e.getKey(), "product", null, e.getValue()));
            }
        }

        Map<DiffType, Long> counts = diffs.stream().collect(Collectors.groupingBy(
                ReconDifference::type, () -> new EnumMap<>(DiffType.class), Collectors.counting()));

        log.info("Recon [{}]: source products={}, target products={}, differences={}",
                hierarchyName, source.size(), target.size(), diffs.size());
        return new ReconReport(hierarchyName, source.size(), target.size(), counts, diffs);
    }

    // -------------------------------------------------------------------------------------
    // On-prem side:  productName -> complete hierarchy path
    // Same rules as ingestion: levels run root -> leaf; a level with the same name (or same on-prem row)
    // as the previous one restarts the chain as a root.
    // -------------------------------------------------------------------------------------
    private Map<String, String> buildSource(List<ProductHierarchyData> products, List<ReconDifference> diffs,
                                            Map<String, ProductHierarchyData> sourceByName) {
        Map<String, String> byProduct = new LinkedHashMap<>();

        for (ProductHierarchyData p : products) {
            String productName = trim(productNameOf(p));
            if (productName == null) {
                diffs.add(new ReconDifference(DiffType.SOURCE_PRODUCT_NAME_MISSING,
                        String.valueOf(p.getProductId()), "productName", null, null));
                continue;
            }

            String path = String.join("/", sourceChain(p, productName, diffs));

            String existing = byProduct.putIfAbsent(productName, path);
            if (existing == null) {
                sourceByName.put(productName, p);   // the first on-prem row of a product is the one compared
            } else if (!existing.equals(path)) {
                diffs.add(new ReconDifference(DiffType.SOURCE_DUPLICATE_PRODUCT, productName, "hierarchyPath",
                        existing, path));
            }
        }
        return byProduct;
    }

    private List<String> sourceChain(ProductHierarchyData p, String productName, List<ReconDifference> diffs) {
        List<String> chain = new ArrayList<>();
        if (p.getHierarchyLevels() == null) return chain;

        String prevName = null;
        String prevRef = null;
        for (NodeLevel l : p.getHierarchyLevels()) {
            String name = trim(l.getNodeName());
            if (name == null) {
                diffs.add(new ReconDifference(DiffType.SOURCE_NODE_NAME_MISSING, productName, "nodeName", null, null));
                continue;
            }
            // The on-prem id is read here only transiently for the self-parent guard (mirrors ingestion);
            // it is never stored and never used as a key.
            String ref = l.getSourceNodeId();
            if (restartsChain(prevName, prevRef, name, ref)) chain.clear();

            chain.add(name);
            prevName = name;
            prevRef = ref;
        }
        return chain;
    }

    /** Self-parent guard (mirrors ingestion): same name or same on-prem row as the previous level => a new root. */
    private static boolean restartsChain(String prevName, String prevRef, String name, String ref) {
        return prevName != null && (name.equals(prevName) || (ref != null && ref.equals(prevRef)));
    }

    /** The on-prem levels that make up the product's hierarchy (same rules as sourceChain, without diffs). */
    private List<NodeLevel> sourceLevelChain(ProductHierarchyData p) {
        List<NodeLevel> chain = new ArrayList<>();
        if (p.getHierarchyLevels() == null) return chain;

        String prevName = null;
        String prevRef = null;
        for (NodeLevel l : p.getHierarchyLevels()) {
            String name = trim(l.getNodeName());
            if (name == null) continue;
            String ref = l.getSourceNodeId();
            if (restartsChain(prevName, prevRef, name, ref)) chain.clear();

            chain.add(l);
            prevName = name;
            prevRef = ref;
        }
        return chain;
    }

    // -------------------------------------------------------------------------------------
    // Target side:  productName -> complete hierarchy path
    // Loads every stored product that belongs to the hierarchy, then reads each product's own
    // productHierarchies (leaf node + its embedded parent chain) to build the path.
    // -------------------------------------------------------------------------------------
    private Map<String, String> loadTarget(String hierarchy, Set<String> hierarchyNames,
                                            Set<String> noHierarchyProducts, List<ReconDifference> diffs,
                                            Map<String, MigrationProduct> targetByName) {
        // Hierarchy names are stored as the on-prem row carried them ("TREASURY", "treasury "), so match every
        // name seen (requested + on-prem rows), case-insensitively, on the whole value.
        String regex = "^\\s*(?:" + hierarchyNames.stream().map(HierarchyNodeService::escapeRegex)
                .collect(Collectors.joining("|")) + ")\\s*$";

        Map<String, MigrationProduct> loaded = new LinkedHashMap<>();
        fetchProducts(regex).forEach(p -> loaded.putIfAbsent(uniqueKey(p), p));
        int byHierarchy = loaded.size();
        fetchByNames(noHierarchyProducts).forEach(p -> loaded.putIfAbsent(uniqueKey(p), p));

        List<MigrationProduct> products = new ArrayList<>(loaded.values());
        log.info("Recon [{}]: loaded {} target products ({} by hierarchy names {}, {} by name for products without hierarchy)",
                hierarchy, products.size(), byHierarchy, hierarchyNames, products.size() - byHierarchy);
        if (byHierarchy == 0) {
            logStoredHierarchyNames(hierarchy);
        }

        Map<String, String> byProduct = new LinkedHashMap<>();
        for (MigrationProduct p : products) {
            String productName = trim(p.getProductName());
            if (productName == null) {
                diffs.add(new ReconDifference(DiffType.TARGET_PRODUCT_NAME_MISSING, productId(p), "productName", null, null));
                continue;
            }

            String path = pathOf(p, productName, diffs);

            String existing = byProduct.putIfAbsent(productName, path);
            if (existing == null) {
                targetByName.put(productName, p);   // the kept (first) copy is the one compared
            } else {
                // a plain Map would overwrite and hide the duplicate - keep the first, report the rest
                diffs.add(new ReconDifference(DiffType.DUPLICATE_PRODUCT, productName, "product",
                        "1 product", "another copy " + productId(p) + " at path '" + path + "' (kept: '" + existing + "')"));
            }
        }
        return byProduct;
    }

    /** Products whose hierarchy name matches; falls back to an in-memory scan if the dot-path query finds nothing. */
    private List<MigrationProduct> fetchProducts(String regex) {
        Document filter = new Document("data.productHierarchies.hierarchyName",
                new Document("$regex", regex).append("$options", "i"));

        List<MigrationProduct> products;
        try (Stream<MigrationProduct> found = productData.findByQuery(filter.toJson())) {
            products = found.collect(Collectors.toList());
        }
        if (!products.isEmpty()) return products;

        // The stored shape of productHierarchies (embedded vs referenced) is not visible from the model alone,
        // so scan in memory rather than trusting the dot-path query to be the only way in.
        log.warn("Recon: dot-path product query returned nothing, scanning products in memory");
        Pattern pattern = Pattern.compile(regex, Pattern.CASE_INSENSITIVE);
        try (Stream<MigrationProduct> all = productData.findByQuery("{}")) {
            return all.limit(10_000)
                    .filter(p -> p.getProductHierarchies() != null && p.getProductHierarchies().stream()
                            .anyMatch(n -> n.getHierarchyName() != null && pattern.matcher(n.getHierarchyName()).matches()))
                    .collect(Collectors.toList());
        }
    }

    /** Stored products with these exact names (used for products that have no hierarchy to filter on). */
    private List<MigrationProduct> fetchByNames(Set<String> names) {
        if (names.isEmpty()) return List.of();
        Document filter = new Document("data.productName", new Document("$in", new ArrayList<>(names)));
        try (Stream<MigrationProduct> found = productData.findByQuery(filter.toJson())) {
            return found.filter(p -> names.contains(trim(p.getProductName()))).collect(Collectors.toList());
        }
    }

    private static String uniqueKey(MigrationProduct p) {
        String id = productId(p);
        return "null".equals(id) ? "obj@" + System.identityHashCode(p) : id;
    }

    /** The product's complete hierarchy path. "" when it has no hierarchy. */
    private String pathOf(MigrationProduct p, String productName, List<ReconDifference> diffs) {
        List<Node> hierarchies = p.getProductHierarchies();
        if (hierarchies == null || hierarchies.isEmpty()) return "";

        List<List<String>> chains = new ArrayList<>();
        for (Node n : hierarchies) {
            if (n != null) chains.add(nameChain(n, productName, diffs));
        }
        if (chains.isEmpty()) return "";

        // One product = one complete hierarchy: the longest chain is the leaf's; every other entry must be
        // one of its ancestors, otherwise the product points at several different hierarchies.
        List<String> longest = chains.stream().max(Comparator.comparingInt(List::size)).get();
        for (List<String> c : chains) {
            if (!longest.subList(0, c.size()).equals(c)) {
                diffs.add(new ReconDifference(DiffType.MULTIPLE_HIERARCHIES, productName, "productHierarchies",
                        null, chains.stream().map(x -> String.join("/", x)).collect(Collectors.joining(" ; "))));
                break;
            }
        }
        return String.join("/", longest);
    }

    /** root -> ... -> node names, by walking the embedded parent chain upwards from this node. */
    private List<String> nameChain(Node start, String productName, List<ReconDifference> diffs) {
        LinkedList<String> chain = new LinkedList<>();
        Set<String> seen = new HashSet<>();
        Node cur = start;

        while (cur != null) {
            String id = mongoId(cur);
            if (!seen.add(visitKey(cur))) {
                diffs.add(new ReconDifference(DiffType.CYCLE, productName, "parent", null, id));
                break;
            }

            String name = trim(cur.getNodeName());
            if (name == null) {
                diffs.add(new ReconDifference(DiffType.INCOMPLETE_HIERARCHY, productName, "node", null,
                        "node " + id + " has no name"));
                break;
            }
            chain.addFirst(name);
            cur = cur.getParentNode();
        }
        return chain;
    }

    // -------------------------------------------------------------------------------------
    // Field-by-field comparison (everything except productName + hierarchy path)
    // -------------------------------------------------------------------------------------

    private void compareProductFields(String product, ProductHierarchyData source, MigrationProduct target,
                                      List<ReconDifference> diffs) {
        for (ProductField f : PRODUCT_FIELDS) {
            String sourceValue = norm(f.source().apply(source));
            String targetValue = norm(f.target().apply(target));
            if (!Objects.equals(sourceValue, targetValue)) {
                diffs.add(new ReconDifference(DiffType.PRODUCT_FIELD_MISMATCH, product, f.name(), sourceValue, targetValue));
            }
        }
    }

    /**
     * Pairs the on-prem levels with the stored nodes position by position (root first) and compares every field
     * ingestion sets from the level. nodeName is the path itself, so it is already covered by the path comparison.
     * lastChangedDateTime / lastChangedUserId are not compared per node: ingestion takes them from whichever product
     * touched a shared node last, so they are not a stable expectation. A node shared by several products is
     * reported once.
     */
    private void compareNodeFields(ProductHierarchyData sourceProduct, MigrationProduct targetProduct,
                                   List<ReconDifference> diffs, Set<String> alreadyReported) {
        List<NodeLevel> levels = sourceLevelChain(sourceProduct);
        List<Node> nodes = leafChain(targetProduct);

        StringBuilder path = new StringBuilder();
        for (int i = 0; i < Math.min(levels.size(), nodes.size()); i++) {
            NodeLevel l = levels.get(i);
            Node n = nodes.get(i);
            if (i > 0) path.append('/');
            path.append(trim(l.getNodeName()));

            List<NodeField> fields = List.of(
                    new NodeField("nodeDescription", l.getNodeName(), n.getNodeDescription()),   // ingestion: description = node name
                    new NodeField("levelNumber", l.getLevel(), n.getLevelNumber()),
                    new NodeField("nodeStatus", ACTIVE_STATUS, n.getNodeStatus()),
                    new NodeField("hierarchyName", l.getHierarchyName(), n.getHierarchyName()),
                    new NodeField("hierarchyType", MIGRATION_PRODUCT_TYPE, n.getHierarchyType()),
                    new NodeField("hierarchyPath", l.getNodePath(), n.getHierarchyPath()));

            for (NodeField f : fields) {
                String sourceValue = norm(f.source());
                String targetValue = norm(f.target());
                if (!Objects.equals(sourceValue, targetValue)
                        && alreadyReported.add(path + "|" + f.name() + "|" + sourceValue + "|" + targetValue)) {
                    diffs.add(new ReconDifference(DiffType.NODE_FIELD_MISMATCH, path.toString(), f.name(),
                            sourceValue, targetValue));
                }
            }
        }
    }

    /** The stored nodes of the product's complete hierarchy, root first (the longest chain, as pathOf uses). */
    private List<Node> leafChain(MigrationProduct p) {
        List<Node> longest = new ArrayList<>();
        if (p.getProductHierarchies() == null) return longest;

        for (Node n : p.getProductHierarchies()) {
            if (n == null) continue;
            List<Node> chain = nodeChain(n);
            if (chain.size() > longest.size()) longest = chain;
        }
        return longest;
    }

    /** root -> ... -> start, following the embedded parents (stops on a cycle). Package-private for direct testing. */
    List<Node> nodeChain(Node start) {
        LinkedList<Node> chain = new LinkedList<>();
        Set<String> seen = new HashSet<>();
        Node cur = start;
        while (cur != null && seen.add(visitKey(cur))) {
            chain.addFirst(cur);
            cur = cur.getParentNode();
        }
        return chain;
    }

    /**
     * Makes values of different types comparable: blank == null, 5 == 5.0 == BigDecimal("5.00"),
     * java.util.Date == Instant at the same moment.
     */
    static String norm(Object v) {
        if (v == null) return null;
        if (v instanceof CharSequence) return trim(v.toString());
        if (v instanceof Date d) return new Date(d.getTime()).toInstant().toString();
        if (v instanceof BigDecimal b) return b.stripTrailingZeros().toPlainString();
        if (v instanceof Number || v instanceof TemporalAccessor) return v.toString();
        return trim(String.valueOf(v));
    }

    private static String visitKey(Node n) {
        String id = mongoId(n);
        return "null".equals(id) ? "obj@" + System.identityHashCode(n) : id;
    }

    /** Hierarchy names to load from the target: the requested one plus whatever the on-prem rows carry. */
    private static Set<String> sourceHierarchyNames(String requested, List<ProductHierarchyData> products) {
        Set<String> names = new LinkedHashSet<>();
        names.add(requested.trim());
        for (ProductHierarchyData p : products) {
            if (p.getHierarchyLevels() == null) continue;
            for (NodeLevel l : p.getHierarchyLevels()) {
                String h = trim(l.getHierarchyName());
                if (h != null) names.add(h);
            }
        }
        return names;
    }

    /** Diagnostic when nothing matched: shows which hierarchy names the stored products really carry. */
    private void logStoredHierarchyNames(String requested) {
        try (Stream<MigrationProduct> all = productData.findByQuery("{}")) {
            Set<String> names = all.limit(5000)
                    .filter(p -> p.getProductHierarchies() != null)
                    .flatMap(p -> p.getProductHierarchies().stream())
                    .map(Node::getHierarchyName)
                    .filter(Objects::nonNull)
                    .collect(Collectors.toCollection(TreeSet::new));
            log.warn("Recon: no target products matched hierarchyName '{}'. Stored hierarchyName values: {}", requested, names);
        } catch (Exception e) {
            log.warn("Recon: no target products matched '{}' and the diagnostic lookup failed: {}", requested, e.getMessage());
        }
    }

    // =====================================================================================
    // helpers
    // =====================================================================================

    /** The on-prem product's name. TODO: adjust to the real getter on ProductHierarchyData. */
    private static String productNameOf(ProductHierarchyData p) {
        return p.getProductName();
    }

    private static String productId(MigrationProduct p) {
        return p.getIdentifier() == null ? "null" : String.valueOf(p.getIdentifier().getId());
    }

    private static String mongoId(Node n) {
        return n.getIdentifier() == null ? "null" : String.valueOf(n.getIdentifier().getId());
    }

    /** Escapes regex metacharacters so a hierarchy name is matched literally. */
    private static String escapeRegex(String s) {
        return s.replaceAll("[\\\\.^$|?*+()\\[\\]{}]", "\\\\$0");
    }

    private static String trim(String s) {
        return (s == null || s.isBlank()) ? null : s.trim();
    }

    private static String nz(String s) {
        return s == null ? "" : s;
    }
}
