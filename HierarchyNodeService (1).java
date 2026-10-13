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

import java.util.*;
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
        SOURCE_DUPLICATE_PRODUCT      // same on-prem product name with two different paths
    }

    public record ReconDifference(DiffType type, String key, String field,
                                  String sourceValue, String targetValue) {}

    public record ReconReport(String hierarchyName, int sourceProductCount, int targetProductCount,
                              Map<DiffType, Long> countsByType, List<ReconDifference> differences) {
        public boolean passed() { return differences.isEmpty(); }
    }

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

        Map<String, String> source = buildSource(sourceProducts, diffs);

        // Products without a hierarchy carry no hierarchy name, so a hierarchy-based query can never find them
        // in the target. Look those up by product name instead.
        Set<String> noHierarchyProducts = source.entrySet().stream()
                .filter(e -> e.getValue().isEmpty()).map(Map.Entry::getKey)
                .collect(Collectors.toCollection(LinkedHashSet::new));

        Map<String, String> target = loadTarget(hierarchyName,
                sourceHierarchyNames(hierarchyName, sourceProducts), noHierarchyProducts, diffs);

        // a product whose parent chain is broken has an unreliable path: it is already reported as
        // INCOMPLETE_HIERARCHY / CYCLE, so do not add a misleading PATH_MISMATCH on top
        Set<String> brokenChains = diffs.stream()
                .filter(d -> d.type() == DiffType.INCOMPLETE_HIERARCHY || d.type() == DiffType.CYCLE)
                .map(ReconDifference::key).collect(Collectors.toSet());

        for (Map.Entry<String, String> e : source.entrySet()) {
            String product = e.getKey();
            if (!target.containsKey(product)) {
                diffs.add(new ReconDifference(DiffType.MISSING_PRODUCT, product, "product", e.getValue(), null));
                continue;
            }
            String targetPath = target.get(product);
            if (!brokenChains.contains(product) && !Objects.equals(e.getValue(), targetPath)) {
                diffs.add(new ReconDifference(DiffType.PATH_MISMATCH, product, "hierarchyPath", e.getValue(), targetPath));
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
    private Map<String, String> buildSource(List<ProductHierarchyData> products, List<ReconDifference> diffs) {
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
            if (existing != null && !existing.equals(path)) {
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
            boolean selfParent = prevName != null
                    && (name.equals(prevName) || (ref != null && ref.equals(prevRef)));
            if (selfParent) chain.clear();

            chain.add(name);
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
                                            Set<String> noHierarchyProducts, List<ReconDifference> diffs) {
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
            if (existing != null) {
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
            String visitKey = "null".equals(id) ? "obj@" + System.identityHashCode(cur) : id;
            if (!seen.add(visitKey)) {
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
