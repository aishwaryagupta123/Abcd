package com.db.rds.platform.prds.sourcing.day0.service;

import com.db.rds.platform.common.models.models.Node;                       // TODO: adjust import
import com.db.rds.platform.common.models.models.PrimaryIdentifier;
import com.db.rds.platform.prds.sourcing.day0.model.MigrationProduct;       // TODO: adjust import
import com.db.rds.platform.prds.sourcing.day0.model.NodeLevel;              // TODO: adjust import
import com.db.rds.platform.prds.sourcing.day0.model.ProductHierarchyData;   // TODO: adjust import
import com.db.rds.platform.prds.sourcing.day0.service.HierarchyNodeService.DiffType;
import com.db.rds.platform.prds.sourcing.day0.service.HierarchyNodeService.ReconDifference;
import com.db.rds.platform.prds.sourcing.day0.service.HierarchyNodeService.ReconReport;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for HierarchyNodeService (line + branch coverage of every method).
 *
 * Plain mocks, no MockitoExtension: the product store is stubbed ONCE in setUp with an answer that routes on the
 * kind of query the service sends, so tests only fill the lists below.
 */
class HierarchyNodeServiceTest {

    private MigrationNodePrimeDataService primeData;
    private MigrationProductPrimeDataService productData;
    private HierarchyNodeService service;

    /** What the mocked product store returns for each kind of query the service issues. */
    private List<MigrationProduct> hierarchyHits;   // query with $regex on productHierarchies.hierarchyName
    private List<MigrationProduct> nameHits;        // query with $in on productName
    private List<MigrationProduct> allProducts;     // the "{}" scan / diagnostic query
    private List<String> queries;                   // every query string received
    private int allQueryCalls;
    private int failAllQueriesFromCall;
    private int seq;

    @BeforeEach
    void setUp() {
        primeData = mock(MigrationNodePrimeDataService.class);
        productData = mock(MigrationProductPrimeDataService.class);
        service = new HierarchyNodeService(primeData, productData);

        hierarchyHits = new ArrayList<>();
        nameHits = new ArrayList<>();
        allProducts = new ArrayList<>();
        queries = new ArrayList<>();
        allQueryCalls = 0;
        failAllQueriesFromCall = Integer.MAX_VALUE;
        seq = 0;

        when(productData.findByQuery(anyString())).thenAnswer(invocation -> {
            String query = invocation.getArgument(0);
            queries.add(query);
            if (query.contains("$regex")) {
                return hierarchyHits.stream();
            }
            if (query.contains("$in")) {
                return nameHits.stream();
            }
            allQueryCalls++;
            if (allQueryCalls >= failAllQueriesFromCall) {
                throw new IllegalStateException("store unavailable");
            }
            return allProducts.stream();
        });
    }

    // =====================================================================================
    // findExistingIdentifier
    // =====================================================================================

    @Test
    void findExistingIdentifier_anyNullArgument_returnsNullWithoutQuerying() {
        // primeData is not stubbed: a query would return null and blow up with an NPE
        assertNull(service.findExistingIdentifier(null, "Mid", "Root/Mid"));
        assertNull(service.findExistingIdentifier("Treasury", null, "Root/Mid"));
        assertNull(service.findExistingIdentifier("Treasury", "Mid", null));
    }

    @Test
    void findExistingIdentifier_singleMatch_returnsItsIdentifierAndQueriesByNameAndPath() {
        PrimaryIdentifier identifier = pid("m1");
        Node existing = mock(Node.class);
        when(existing.getIdentifier()).thenReturn(identifier);
        List<String> seen = new ArrayList<>();
        when(primeData.findByQuery(anyString())).thenAnswer(invocation -> {
            String query = invocation.getArgument(0);
            seen.add(query);
            return Stream.of(existing);
        });

        assertSame(identifier, service.findExistingIdentifier("Treasury", "Mid", "Root/Mid"));

        assertEquals(1, seen.size());
        assertTrue(seen.get(0).contains("data.hierarchyName"));
        assertTrue(seen.get(0).contains("data.nodeName"));
        assertTrue(seen.get(0).contains("data.hierarchyPath"));
    }

    @Test
    void findExistingIdentifier_noMatch_returnsNull() {
        when(primeData.findByQuery(anyString())).thenAnswer(invocation -> Stream.empty());

        assertNull(service.findExistingIdentifier("Treasury", "Mid", "Root/Mid"));
    }

    @Test
    void findExistingIdentifier_severalMatches_returnsTheFirst() {
        PrimaryIdentifier first = pid("m1");
        PrimaryIdentifier second = pid("m2");
        Node n1 = mock(Node.class);
        Node n2 = mock(Node.class);
        when(n1.getIdentifier()).thenReturn(first);
        when(n2.getIdentifier()).thenReturn(second);
        when(primeData.findByQuery(anyString())).thenAnswer(invocation -> Stream.of(n1, n2));

        assertSame(first, service.findExistingIdentifier("Treasury", "Mid", "Root/Mid"));
    }

    @Test
    void findExistingIdentifier_queryFails_isRethrownNotSwallowed() {
        RuntimeException dbDown = new RuntimeException("db down");
        when(primeData.findByQuery(anyString())).thenThrow(dbDown);

        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> service.findExistingIdentifier("Treasury", "Mid", "Root/Mid"));

        assertTrue(e.getMessage().contains("Treasury/Mid/Root/Mid"));
        assertSame(dbDown, e.getCause());
    }

    // =====================================================================================
    // naturalKey
    // =====================================================================================

    @Test
    void naturalKey_joinsHierarchyNameAndPath_andTreatsNullAsEmpty() {
        assertEquals("Treasury|Mid|Root/Mid", HierarchyNodeService.naturalKey("Treasury", "Mid", "Root/Mid"));
        assertEquals("||", HierarchyNodeService.naturalKey(null, null, null));
    }

    // =====================================================================================
    // reconcile: matching / differences
    // =====================================================================================

    @Test
    void reconcile_identicalHierarchies_passes() {
        hierarchyHits.add(product("p1", "ProdA", chain("Treasury", "Root", "Mid", "Leaf")));

        ReconReport r = run("Treasury", source("ProdA", lvl("Root"), lvl("Mid"), lvl("Leaf")));

        assertTrue(r.passed());
        assertEquals("Treasury", r.hierarchyName());
        assertEquals(1, r.sourceProductCount());
        assertEquals(1, r.targetProductCount());
        assertTrue(r.countsByType().isEmpty());
        assertTrue(queries.stream().noneMatch(q -> q.contains("$in")), "nothing to look up by name");
    }

    @Test
    void reconcile_reportsMissingExtraPathMismatchAndDuplicateProducts() {
        hierarchyHits.add(product("p1", "ProdA", chain("Treasury", "Root", "Mid", "Leaf")));
        hierarchyHits.add(product("p2", "ProdA", chain("Treasury", "Root", "Mid", "Leaf")));   // stray copy
        hierarchyHits.add(product("p3", "ProdB", chain("Treasury", "Root", "Mid", "Other")));  // wrong parent
        hierarchyHits.add(product("p4", "ProdD", chain("Treasury", "Root", "Zed")));           // not on-prem

        ReconReport r = run("Treasury",
                source("ProdA", lvl("Root"), lvl("Mid"), lvl("Leaf")),
                source("ProdB", lvl("Root"), lvl("Other")),
                source("ProdC", lvl("Root"), lvl("Moved")));                                    // not migrated

        assertFalse(r.passed());
        assertEquals(3, r.sourceProductCount());
        assertEquals(3, r.targetProductCount());   // ProdA (first copy), ProdB, ProdD

        ReconDifference mismatch = only(r, DiffType.PATH_MISMATCH);
        assertEquals("ProdB", mismatch.key());
        assertEquals("Root/Other", mismatch.sourceValue());
        assertEquals("Root/Mid/Other", mismatch.targetValue());

        ReconDifference missing = only(r, DiffType.MISSING_PRODUCT);
        assertEquals("ProdC", missing.key());
        assertEquals("Root/Moved", missing.sourceValue());
        assertNull(missing.targetValue());

        ReconDifference extra = only(r, DiffType.EXTRA_PRODUCT);
        assertEquals("ProdD", extra.key());
        assertEquals("Root/Zed", extra.targetValue());

        ReconDifference duplicate = only(r, DiffType.DUPLICATE_PRODUCT);
        assertEquals("ProdA", duplicate.key());
        assertTrue(duplicate.targetValue().contains("p2"));

        assertEquals(Long.valueOf(1), r.countsByType().get(DiffType.PATH_MISMATCH));
        assertEquals(4, r.differences().size());
    }

    // =====================================================================================
    // buildSource
    // =====================================================================================

    @Test
    void buildSource_productWithoutName_isReportedAndSkipped() {
        ReconReport r = run("Treasury", source(null, lvl("Root")), source("  ", lvl("Root")));

        assertEquals(2, ofType(r, DiffType.SOURCE_PRODUCT_NAME_MISSING).size());
        assertEquals(0, r.sourceProductCount());
    }

    @Test
    void buildSource_sameProductTwice_samePathIsIgnored_differentPathIsReported() {
        hierarchyHits.add(product("p1", "Dup", chain("Treasury", "Root", "A")));

        ReconReport r = run("Treasury",
                source("Dup", lvl("Root"), lvl("A")),
                source("Dup", lvl("Root"), lvl("A")),      // identical, no diff
                source("Dup", lvl("Root"), lvl("B")));     // conflicting path

        assertEquals(1, r.sourceProductCount());
        ReconDifference conflict = only(r, DiffType.SOURCE_DUPLICATE_PRODUCT);
        assertEquals("Dup", conflict.key());
        assertEquals("Root/A", conflict.sourceValue());
        assertEquals("Root/B", conflict.targetValue());
        assertTrue(ofType(r, DiffType.PATH_MISMATCH).isEmpty(), "the first on-prem path is the one compared");
    }

    @Test
    void buildSource_selfParentGuardAndMissingNodeNames_buildTheExpectedPaths() {
        // restart because the node name repeats
        ProductHierarchyData sameName = source("SameName", lvl("Root"), lvl("Root"));
        // restart because the on-prem row repeats (names differ)
        ProductHierarchyData sameRow = source("SameRow", level("Treasury", "Root", "r1"), level("Treasury", "Mid", "r1"));
        // different rows and names: no restart
        ProductHierarchyData differentRows = source("DifferentRows", level("Treasury", "Root", "r1"), level("Treasury", "Mid", "r2"));
        // no row ids at all, hierarchy name missing on the first level
        ProductHierarchyData noRefs = source("NoRefs", level(null, "Root", null), level("Treasury", "Mid", null));
        // no levels at all
        ProductHierarchyData noLevels = source("NoLevels", (NodeLevel[]) null);
        // a level without a node name is skipped and reported
        ProductHierarchyData nameless = source("Nameless", lvl("Root"), level("Treasury", null, "r9"), lvl("Leaf"));

        hierarchyHits.add(product("p1", "SameName", chain("Treasury", "Root")));
        hierarchyHits.add(product("p2", "SameRow", chain("Treasury", "Mid")));
        hierarchyHits.add(product("p3", "DifferentRows", chain("Treasury", "Root", "Mid")));
        hierarchyHits.add(product("p4", "NoRefs", chain("Treasury", "Root", "Mid")));
        hierarchyHits.add(product("p5", "Nameless", chain("Treasury", "Root", "Leaf")));
        nameHits.add(product("p6", "NoLevels", (Node[]) null));

        ReconReport r = run("Treasury", sameName, sameRow, differentRows, noRefs, noLevels, nameless);

        assertEquals(1, r.differences().size(), r.differences().toString());
        assertEquals("Nameless", only(r, DiffType.SOURCE_NODE_NAME_MISSING).key());
        assertEquals(6, r.sourceProductCount());
    }

    // =====================================================================================
    // loadTarget / fetchProducts / fetchByNames
    // =====================================================================================

    @Test
    void loadTarget_productWithoutHierarchy_isLookedUpByNameAndDeduplicated() {
        MigrationProduct withHierarchy = product("p1", "ProdA", chain("Treasury", "Root", "Mid", "Leaf"));
        MigrationProduct withoutHierarchy = product("p2", "ProdF", (Node[]) null);
        MigrationProduct unrelated = product("p3", "Zzz", (Node[]) null);
        hierarchyHits.add(withHierarchy);
        nameHits.add(withoutHierarchy);
        nameHits.add(withHierarchy);   // already loaded by hierarchy: must not be counted twice
        nameHits.add(unrelated);       // name was not asked for: must be filtered out

        ReconReport r = run("Treasury",
                source("ProdA", lvl("Root"), lvl("Mid"), lvl("Leaf")),
                source("ProdF"));

        assertTrue(r.passed(), r.differences().toString());
        assertEquals(2, r.targetProductCount());
        assertTrue(queries.stream().anyMatch(q -> q.contains("$in") && q.contains("ProdF")));
    }

    @Test
    void loadTarget_dotPathQueryFindsNothing_fallsBackToScanningInMemory() {
        allProducts.add(product("p1", "ProdA", chain("TREASURY ", "Root", "Mid", "Leaf")));   // stored with other casing/space
        allProducts.add(product("p2", "Other", chain("Equities", "Eq")));                     // another hierarchy

        ReconReport r = run("Treasury", source("ProdA", lvl("Root"), lvl("Mid"), lvl("Leaf")));

        assertTrue(r.passed(), r.differences().toString());
        assertEquals(1, r.targetProductCount());
        assertTrue(queries.contains("{}"));
    }

    @Test
    void loadTarget_nothingMatches_reportsMissingAndLogsStoredHierarchyNames() {
        allProducts.add(product("p1", "NoHierarchy", (Node[]) null));
        allProducts.add(product("p2", "NoHierarchyName", node("n1", null, "Root", null)));
        allProducts.add(product("p3", "Equities1", chain("Equities", "Eq")));

        ReconReport r = run("Treasury", source("ProdA", lvl("Root")));

        assertEquals(0, r.targetProductCount());
        assertEquals("ProdA", only(r, DiffType.MISSING_PRODUCT).key());
        assertEquals(2L, queries.stream().filter(q -> q.equals("{}")).count(), "fallback scan + diagnostic scan");
    }

    @Test
    void loadTarget_diagnosticLookupFails_stillReturnsTheReport() {
        failAllQueriesFromCall = 2;   // 1st "{}" (fallback scan) works, 2nd (diagnostic) throws

        ReconReport r = run("Treasury", source("ProdA", lvl("Root")));

        assertEquals("ProdA", only(r, DiffType.MISSING_PRODUCT).key());
        assertEquals(0, r.targetProductCount());
        assertEquals(2, allQueryCalls);
    }

    @Test
    void loadTarget_productWithoutName_isReported() {
        hierarchyHits.add(product(null, null, chain("Treasury", "Root")));        // no identifier either
        hierarchyHits.add(product("p7", "  ", chain("Treasury", "Root")));

        ReconReport r = run("Treasury", source("ProdA", lvl("Root")));

        List<ReconDifference> nameless = ofType(r, DiffType.TARGET_PRODUCT_NAME_MISSING);
        assertEquals(2, nameless.size());
        assertEquals(new HashSet<>(Arrays.asList("null", "p7")),
                nameless.stream().map(ReconDifference::key).collect(Collectors.toSet()));
        assertEquals("ProdA", only(r, DiffType.MISSING_PRODUCT).key());
    }

    @Test
    void loadTarget_hierarchyNamesAreEscapedBeforeBuildingTheRegex() {
        String special = "A.b^c$d|e?f*g+h(i)j[k]l{m}n\\o";

        run("Treasury", source("ProdA", level(special, "Root", null)));

        String regexQuery = queries.stream().filter(q -> q.contains("$regex")).findFirst().get();
        assertTrue(regexQuery.contains("Treasury"));
        assertFalse(regexQuery.contains("(i)"), "'(' and ')' must be escaped");
        assertFalse(regexQuery.contains("{m}"), "'{' and '}' must be escaped");
        assertTrue(regexQuery.contains("i\\"), "an escape backslash follows the escaped character");
    }

    // =====================================================================================
    // pathOf / nameChain
    // =====================================================================================

    @Test
    void pathOf_noUsableHierarchy_isAnEmptyPath() {
        hierarchyHits.add(product("p1", "EmptyList", new Node[0]));
        hierarchyHits.add(product("p2", "OnlyNullNode", new Node[]{null}));

        ReconReport r = run("Treasury", source("EmptyList"), source("OnlyNullNode"));

        assertTrue(r.passed(), r.differences().toString());
    }

    @Test
    void pathOf_ancestorsListedBesideTheLeaf_usesTheLongestChain() {
        Node leaf = chain("Treasury", "Root", "Mid", "Leaf");
        Node mid = leaf.getParentNode();
        hierarchyHits.add(product("p1", "ProdA", mid, leaf, null));   // ancestor, leaf and a null entry

        ReconReport r = run("Treasury", source("ProdA", lvl("Root"), lvl("Mid"), lvl("Leaf")));

        assertTrue(r.passed(), r.differences().toString());
    }

    @Test
    void pathOf_unrelatedHierarchies_isReportedAsMultipleHierarchies() {
        hierarchyHits.add(product("p1", "ProdA",
                chain("Treasury", "Root", "Mid", "Leaf"), chain("Treasury", "Root", "Other")));

        ReconReport r = run("Treasury", source("ProdA", lvl("Root"), lvl("Mid"), lvl("Leaf")));

        ReconDifference multiple = only(r, DiffType.MULTIPLE_HIERARCHIES);
        assertEquals("ProdA", multiple.key());
        assertEquals("Root/Mid/Leaf ; Root/Other", multiple.targetValue());
        assertTrue(ofType(r, DiffType.PATH_MISMATCH).isEmpty(), "the longest chain is the one compared");
    }

    @Test
    void nameChain_parentWithoutName_isIncompleteAndDoesNotAlsoReportAPathMismatch() {
        Node nameless = node("x2", "Treasury", null, null);
        Node leaf = node("x1", "Treasury", "Leaf", nameless);
        hierarchyHits.add(product("p1", "ProdA", leaf));

        ReconReport r = run("Treasury", source("ProdA", lvl("Root"), lvl("Leaf")));

        ReconDifference incomplete = only(r, DiffType.INCOMPLETE_HIERARCHY);
        assertEquals("ProdA", incomplete.key());
        assertEquals("node x2 has no name", incomplete.targetValue());
        assertTrue(ofType(r, DiffType.PATH_MISMATCH).isEmpty());
    }

    @Test
    void nameChain_cycleThroughIdentifiedNodes_isReported() {
        Node a = node("c1", "Treasury", "A", null);
        Node b = node("c2", "Treasury", "B", a);
        when(a.getParentNode()).thenReturn(b);   // A -> B -> A
        hierarchyHits.add(product("p1", "ProdA", b));

        ReconReport r = run("Treasury", source("ProdA", lvl("A"), lvl("B")));

        ReconDifference cycle = only(r, DiffType.CYCLE);
        assertEquals("ProdA", cycle.key());
        assertEquals("c2", cycle.targetValue());
        assertTrue(ofType(r, DiffType.PATH_MISMATCH).isEmpty());
    }

    @Test
    void nameChain_cycleThroughNodesWithoutIdentifier_isReportedByObjectIdentity() {
        Node loop = node(null, "Treasury", "X", null);
        when(loop.getParentNode()).thenReturn(loop);   // X -> X, no identifier to compare
        hierarchyHits.add(product("p1", "ProdX", loop));

        ReconReport r = run("Treasury", source("ProdX", lvl("X")));

        ReconDifference cycle = only(r, DiffType.CYCLE);
        assertEquals("ProdX", cycle.key());
        assertEquals("null", cycle.targetValue());
    }

    // =====================================================================================
    // report
    // =====================================================================================

    @Test
    void reconReport_passedIsTrueOnlyWithoutDifferences() {
        assertTrue(new ReconReport("H", 0, 0, java.util.Map.of(), List.of()).passed());
        assertFalse(new ReconReport("H", 0, 0, java.util.Map.of(),
                List.of(new ReconDifference(DiffType.MISSING_PRODUCT, "k", "f", null, null))).passed());
    }

    // =====================================================================================
    // helpers
    // =====================================================================================

    private ReconReport run(String hierarchy, ProductHierarchyData... sources) {
        return service.reconcile(hierarchy, Arrays.asList(sources));
    }

    private static List<ReconDifference> ofType(ReconReport report, DiffType type) {
        return report.differences().stream().filter(d -> d.type() == type).collect(Collectors.toList());
    }

    private static ReconDifference only(ReconReport report, DiffType type) {
        List<ReconDifference> found = ofType(report, type);
        assertEquals(1, found.size(), "expected exactly one " + type + " in " + report.differences());
        return found.get(0);
    }

    /** NOTE: stub the nested mock BEFORE passing it to thenReturn(...) - nested when() inside thenReturn breaks Mockito. */
    private static PrimaryIdentifier pid(String id) {
        PrimaryIdentifier identifier = mock(PrimaryIdentifier.class);
        when(identifier.getId()).thenReturn(id);
        return identifier;
    }

    /** A node mock; id == null means "no identifier". */
    private static Node node(String id, String hierarchy, String name, Node parent) {
        Node n = mock(Node.class);
        if (id != null) {
            PrimaryIdentifier identifier = pid(id);
            when(n.getIdentifier()).thenReturn(identifier);
        }
        when(n.getHierarchyName()).thenReturn(hierarchy);
        when(n.getNodeName()).thenReturn(name);
        when(n.getParentNode()).thenReturn(parent);
        return n;
    }

    /** Builds root -> ... -> leaf and returns the LEAF; each node gets a unique id. */
    private Node chain(String hierarchy, String... names) {
        Node parent = null;
        for (String name : names) {
            parent = node("n" + (++seq), hierarchy, name, parent);
        }
        return parent;
    }

    private static MigrationProduct product(String id, String name, Node... hierarchies) {
        MigrationProduct p = mock(MigrationProduct.class);
        if (id != null) {
            PrimaryIdentifier identifier = pid(id);
            when(p.getIdentifier()).thenReturn(identifier);
        }
        when(p.getProductName()).thenReturn(name);
        List<Node> list = hierarchies == null ? null : Arrays.asList(hierarchies);
        when(p.getProductHierarchies()).thenReturn(list);
        return p;
    }

    private static NodeLevel level(String hierarchy, String name, String sourceRef) {
        NodeLevel l = mock(NodeLevel.class);
        when(l.getHierarchyName()).thenReturn(hierarchy);
        when(l.getNodeName()).thenReturn(name);
        when(l.getSourceNodeId()).thenReturn(sourceRef);
        return l;
    }

    private static NodeLevel lvl(String name) {
        return level("Treasury", name, null);
    }

    private static ProductHierarchyData source(String productName, NodeLevel... levels) {
        ProductHierarchyData p = mock(ProductHierarchyData.class);
        when(p.getProductName()).thenReturn(productName);
        List<NodeLevel> list = levels == null ? null : Arrays.asList(levels);
        when(p.getHierarchyLevels()).thenReturn(list);
        return p;
    }
}
