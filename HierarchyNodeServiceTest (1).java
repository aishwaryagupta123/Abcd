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

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.db.rds.platform.prds.sourcing.day0.service.MigrationProductHierarchyService.ACTIVE_STATUS;            // TODO: adjust
import static com.db.rds.platform.prds.sourcing.day0.service.MigrationProductHierarchyService.MIGRATION_PRODUCT_TYPE;   // TODO: adjust
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
 *
 * Fixture rule: helpers build on-prem levels and stored nodes that AGREE on every field (name, description, level,
 * status, hierarchy, type, path), so a test only has to introduce the one difference it is about.
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
    // naturalKey / norm
    // =====================================================================================

    @Test
    void naturalKey_joinsHierarchyNameAndPath_andTreatsNullAsEmpty() {
        assertEquals("Treasury|Mid|Root/Mid", HierarchyNodeService.naturalKey("Treasury", "Mid", "Root/Mid"));
        assertEquals("||", HierarchyNodeService.naturalKey(null, null, null));
    }

    @Test
    void norm_makesValuesOfDifferentTypesComparable() {
        assertNull(HierarchyNodeService.norm(null));
        assertNull(HierarchyNodeService.norm("   "));
        assertNull(HierarchyNodeService.norm(new StringBuilder(" ")));
        assertEquals("a", HierarchyNodeService.norm("  a "));

        assertEquals("1970-01-01T00:00:00Z", HierarchyNodeService.norm(new Date(0)));
        assertEquals(HierarchyNodeService.norm(new Date(0)), HierarchyNodeService.norm(Instant.EPOCH));

        assertEquals("5", HierarchyNodeService.norm(new BigDecimal("5.00")));
        assertEquals("100", HierarchyNodeService.norm(new BigDecimal("1E+2")));
        assertEquals("0", HierarchyNodeService.norm(new BigDecimal("0.00")));
        assertEquals("5", HierarchyNodeService.norm(5L));
        assertEquals(HierarchyNodeService.norm(5), HierarchyNodeService.norm(new BigDecimal("5.0")));

        assertEquals("true", HierarchyNodeService.norm(Boolean.TRUE));
    }

    // =====================================================================================
    // reconcile: matching / differences (productName -> hierarchy path)
    // =====================================================================================

    @Test
    void reconcile_identicalProducts_passes() {
        hierarchyHits.add(product("p1", "ProdA", chain("Treasury", "Root", "Mid", "Leaf")));

        ReconReport r = run("Treasury", source("ProdA", path("Root", "Mid", "Leaf")));

        assertTrue(r.passed(), r.differences().toString());
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
                source("ProdA", path("Root", "Mid", "Leaf")),
                source("ProdB", path("Root", "Other")),
                source("ProdC", path("Root", "Moved")));                                        // not migrated

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
        ReconReport r = run("Treasury", source(null, path("Root")), source("  ", path("Root")));

        assertEquals(2, ofType(r, DiffType.SOURCE_PRODUCT_NAME_MISSING).size());
        assertEquals(0, r.sourceProductCount());
    }

    @Test
    void buildSource_sameProductTwice_samePathIsIgnored_differentPathIsReported() {
        hierarchyHits.add(product("p1", "Dup", chain("Treasury", "Root", "A")));

        ReconReport r = run("Treasury",
                source("Dup", path("Root", "A")),
                source("Dup", path("Root", "A")),      // identical, no diff
                source("Dup", path("Root", "B")));     // conflicting path

        assertEquals(1, r.sourceProductCount());
        ReconDifference conflict = only(r, DiffType.SOURCE_DUPLICATE_PRODUCT);
        assertEquals("Dup", conflict.key());
        assertEquals("Root/A", conflict.sourceValue());
        assertEquals("Root/B", conflict.targetValue());
        assertTrue(ofType(r, DiffType.PATH_MISMATCH).isEmpty(), "the first on-prem path is the one compared");
    }

    @Test
    void buildSource_selfParentGuardAndMissingNodeNames_buildTheExpectedPathsAndNodes() {
        // restart because the node name repeats
        NodeLevel sameName2 = levelAt("Treasury", "Root", null, 2, "Root/Root");
        ProductHierarchyData sameName = source("SameName", levelAt("Treasury", "Root", null, 1, "Root"), sameName2);
        // restart because the on-prem row repeats (names differ)
        NodeLevel sameRow2 = levelAt("Treasury", "Mid", "r1", 2, "Root/Mid");
        ProductHierarchyData sameRow = source("SameRow", levelAt("Treasury", "Root", "r1", 1, "Root"), sameRow2);
        // different rows and names: no restart
        NodeLevel diffRoot = levelAt("Treasury", "Root", "r1", 1, "Root");
        NodeLevel diffMid = levelAt("Treasury", "Mid", "r2", 2, "Root/Mid");
        ProductHierarchyData differentRows = source("DifferentRows", diffRoot, diffMid);
        // no row ids at all, hierarchy name missing on the first level
        NodeLevel noRefRoot = levelAt(null, "Root", null, 1, "Root");
        NodeLevel noRefMid = levelAt("Treasury", "Mid", null, 2, "Root/Mid");
        ProductHierarchyData noRefs = source("NoRefs", noRefRoot, noRefMid);
        // no levels at all
        ProductHierarchyData noLevels = source("NoLevels", (NodeLevel[]) null);
        // a level without a node name is skipped and reported
        NodeLevel namelessRoot = levelAt("Treasury", "Root", null, 1, "Root");
        NodeLevel namelessLeaf = levelAt("Treasury", "Leaf", null, 3, "Root/Leaf");
        ProductHierarchyData nameless = source("Nameless", namelessRoot, levelAt("Treasury", null, "r9", 2, "x"), namelessLeaf);

        hierarchyHits.add(product("p1", "SameName", targetOf(sameName2)));
        hierarchyHits.add(product("p2", "SameRow", targetOf(sameRow2)));
        hierarchyHits.add(product("p3", "DifferentRows", targetOf(diffRoot, diffMid)));
        hierarchyHits.add(product("p4", "NoRefs", targetOf(noRefRoot, noRefMid)));
        hierarchyHits.add(product("p5", "Nameless", targetOf(namelessRoot, namelessLeaf)));
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

        ReconReport r = run("Treasury", source("ProdA", path("Root", "Mid", "Leaf")), source("ProdF"));

        assertTrue(r.passed(), r.differences().toString());
        assertEquals(2, r.targetProductCount());
        assertTrue(queries.stream().anyMatch(q -> q.contains("$in") && q.contains("ProdF")));
    }

    @Test
    void loadTarget_dotPathQueryFindsNothing_fallsBackToScanningInMemory() {
        allProducts.add(product("p1", "ProdA", chain("TREASURY ", "Root", "Mid", "Leaf")));   // stored with other casing/space
        allProducts.add(product("p2", "Other", chain("Equities", "Eq")));                     // another hierarchy

        ReconReport r = run("Treasury", source("ProdA", path("Root", "Mid", "Leaf")));

        assertEquals(1, r.targetProductCount());
        assertTrue(queries.contains("{}"));
        // the product IS loaded and paired; the only thing that differs is the stored hierarchy name's casing
        assertTrue(ofType(r, DiffType.MISSING_PRODUCT).isEmpty());
        assertTrue(ofType(r, DiffType.EXTRA_PRODUCT).isEmpty());
        assertTrue(ofType(r, DiffType.PATH_MISMATCH).isEmpty());
        assertEquals(3, ofType(r, DiffType.NODE_FIELD_MISMATCH).size());
        assertTrue(ofType(r, DiffType.NODE_FIELD_MISMATCH).stream().allMatch(d -> d.field().equals("hierarchyName")));
    }

    @Test
    void loadTarget_nothingMatches_reportsMissingAndLogsStoredHierarchyNames() {
        allProducts.add(product("p1", "NoHierarchy", (Node[]) null));
        allProducts.add(product("p2", "NoHierarchyName", node("n1", null, "Root", null)));
        allProducts.add(product("p3", "Equities1", chain("Equities", "Eq")));

        ReconReport r = run("Treasury", source("ProdA", path("Root")));

        assertEquals(0, r.targetProductCount());
        assertEquals("ProdA", only(r, DiffType.MISSING_PRODUCT).key());
        assertEquals(2L, queries.stream().filter(q -> q.equals("{}")).count(), "fallback scan + diagnostic scan");
    }

    @Test
    void loadTarget_diagnosticLookupFails_stillReturnsTheReport() {
        failAllQueriesFromCall = 2;   // 1st "{}" (fallback scan) works, 2nd (diagnostic) throws

        ReconReport r = run("Treasury", source("ProdA", path("Root")));

        assertEquals("ProdA", only(r, DiffType.MISSING_PRODUCT).key());
        assertEquals(0, r.targetProductCount());
        assertEquals(2, allQueryCalls);
    }

    @Test
    void loadTarget_productWithoutName_isReported() {
        hierarchyHits.add(product(null, null, chain("Treasury", "Root")));        // no identifier either
        hierarchyHits.add(product("p7", "  ", chain("Treasury", "Root")));

        ReconReport r = run("Treasury", source("ProdA", path("Root")));

        List<ReconDifference> nameless = ofType(r, DiffType.TARGET_PRODUCT_NAME_MISSING);
        assertEquals(2, nameless.size());
        assertEquals(new HashSet<>(Arrays.asList("null", "p7")),
                nameless.stream().map(ReconDifference::key).collect(Collectors.toSet()));
        assertEquals("ProdA", only(r, DiffType.MISSING_PRODUCT).key());
    }

    @Test
    void loadTarget_hierarchyNamesAreEscapedBeforeBuildingTheRegex() {
        String special = "A.b^c$d|e?f*g+h(i)j[k]l{m}n\\o";

        run("Treasury", source("ProdA", levelAt(special, "Root", null, 1, "Root")));

        String regexQuery = queries.stream().filter(q -> q.contains("$regex")).findFirst().get();
        assertTrue(regexQuery.contains("Treasury"));
        assertFalse(regexQuery.contains("(i)"), "'(' and ')' must be escaped");
        assertFalse(regexQuery.contains("{m}"), "'{' and '}' must be escaped");
        assertTrue(regexQuery.contains("i\\"), "an escape backslash follows the escaped character");
    }

    // =====================================================================================
    // pathOf / nameChain / nodeChain
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

        ReconReport r = run("Treasury", source("ProdA", path("Root", "Mid", "Leaf")));

        assertTrue(r.passed(), r.differences().toString());
    }

    @Test
    void pathOf_unrelatedHierarchies_isReportedAsMultipleHierarchies() {
        hierarchyHits.add(product("p1", "ProdA",
                chain("Treasury", "Root", "Mid", "Leaf"), chain("Treasury", "Root", "Other")));

        ReconReport r = run("Treasury", source("ProdA", path("Root", "Mid", "Leaf")));

        ReconDifference multiple = only(r, DiffType.MULTIPLE_HIERARCHIES);
        assertEquals("ProdA", multiple.key());
        assertEquals("Root/Mid/Leaf ; Root/Other", multiple.targetValue());
        assertTrue(ofType(r, DiffType.PATH_MISMATCH).isEmpty(), "the longest chain is the one compared");
    }

    @Test
    void nameChain_parentWithoutName_isIncompleteAndDoesNotAlsoReportPathOrNodeMismatches() {
        Node nameless = node("x2", "Treasury", null, null);
        Node leaf = node("x1", "Treasury", "Leaf", nameless);
        hierarchyHits.add(product("p1", "ProdA", leaf));

        ReconReport r = run("Treasury", source("ProdA", path("Root", "Leaf")));

        ReconDifference incomplete = only(r, DiffType.INCOMPLETE_HIERARCHY);
        assertEquals("ProdA", incomplete.key());
        assertEquals("node x2 has no name", incomplete.targetValue());
        assertTrue(ofType(r, DiffType.PATH_MISMATCH).isEmpty());
        assertTrue(ofType(r, DiffType.NODE_FIELD_MISMATCH).isEmpty());
    }

    @Test
    void nameChain_cycleThroughIdentifiedNodes_isReported() {
        Node a = node("c1", "Treasury", "A", null);
        Node b = node("c2", "Treasury", "B", a);
        when(a.getParentNode()).thenReturn(b);   // A -> B -> A
        hierarchyHits.add(product("p1", "ProdA", b));

        ReconReport r = run("Treasury", source("ProdA", path("A", "B")));

        ReconDifference cycle = only(r, DiffType.CYCLE);
        assertEquals("ProdA", cycle.key());
        assertEquals("c2", cycle.targetValue());
        assertTrue(ofType(r, DiffType.PATH_MISMATCH).isEmpty());
        assertTrue(ofType(r, DiffType.NODE_FIELD_MISMATCH).isEmpty());
    }

    @Test
    void nameChain_cycleThroughNodesWithoutIdentifier_isReportedByObjectIdentity() {
        Node loop = node(null, "Treasury", "X", null);
        when(loop.getParentNode()).thenReturn(loop);   // X -> X, no identifier to compare
        hierarchyHits.add(product("p1", "ProdX", loop));

        ReconReport r = run("Treasury", source("ProdX", path("X")));

        ReconDifference cycle = only(r, DiffType.CYCLE);
        assertEquals("ProdX", cycle.key());
        assertEquals("null", cycle.targetValue());
    }

    @Test
    void nodeChain_stopsWhenTheParentChainLoops() {
        Node a = node("c1", "Treasury", "A", null);
        Node b = node("c2", "Treasury", "B", a);
        when(a.getParentNode()).thenReturn(b);

        List<Node> chain = service.nodeChain(b);

        assertEquals(2, chain.size());
        assertSame(a, chain.get(0));
        assertSame(b, chain.get(1));
    }

    // =====================================================================================
    // product fields
    // =====================================================================================

    @Test
    void productFields_differencesAreReported_andValuesAreNormalisedBeforeComparing() {
        ProductHierarchyData src = source("ProdA", path("Root"));
        when(src.getProductType()).thenReturn(" BOND ");        // trimmed => equal to "BOND"
        when(src.getProductDescription()).thenReturn("Desc");
        when(src.getCode()).thenReturn("C1");
        when(src.getLongName()).thenReturn("   ");              // blank => equal to null

        MigrationProduct stored = product("p1", "ProdA", chain("Treasury", "Root"));
        when(stored.getProductType()).thenReturn("BOND");
        when(stored.getProductDescription()).thenReturn("Other");
        when(stored.getActiveDate()).thenReturn(Instant.parse("2024-01-01T00:00:00Z"));
        hierarchyHits.add(stored);

        ReconReport r = run("Treasury", src);

        List<ReconDifference> diffs = ofType(r, DiffType.PRODUCT_FIELD_MISMATCH);
        assertEquals(r.differences().size(), diffs.size(), "nothing but product field differences expected");
        assertEquals(new HashSet<>(Arrays.asList("productDescription", "code", "activeDate")),
                diffs.stream().map(ReconDifference::field).collect(Collectors.toSet()));

        ReconDifference description = diffs.stream().filter(d -> d.field().equals("productDescription")).findFirst().get();
        assertEquals("ProdA", description.key());
        assertEquals("Desc", description.sourceValue());
        assertEquals("Other", description.targetValue());

        ReconDifference code = diffs.stream().filter(d -> d.field().equals("code")).findFirst().get();
        assertEquals("C1", code.sourceValue());
        assertNull(code.targetValue());

        ReconDifference activeDate = diffs.stream().filter(d -> d.field().equals("activeDate")).findFirst().get();
        assertNull(activeDate.sourceValue());
        assertEquals("2024-01-01T00:00:00Z", activeDate.targetValue());
    }

    @Test
    void productFields_withDuplicatedProducts_theFirstCopyOnEachSideIsTheOneCompared() {
        ProductHierarchyData firstRow = source("Dup", path("Root"));
        when(firstRow.getCode()).thenReturn("C1");
        ProductHierarchyData secondRow = source("Dup", path("Root"));
        when(secondRow.getCode()).thenReturn("OTHER");
        MigrationProduct keptCopy = product("p1", "Dup", chain("Treasury", "Root"));
        when(keptCopy.getCode()).thenReturn("C1");
        MigrationProduct strayCopy = product("p2", "Dup", chain("Treasury", "Root"));
        when(strayCopy.getCode()).thenReturn("ZZZ");
        hierarchyHits.add(keptCopy);
        hierarchyHits.add(strayCopy);

        ReconReport r = run("Treasury", firstRow, secondRow);

        assertEquals(1, r.differences().size(), r.differences().toString());
        assertEquals("Dup", only(r, DiffType.DUPLICATE_PRODUCT).key());
    }

    @Test
    void productFields_areStillComparedWhenThePathDiffers() {
        ProductHierarchyData src = source("ProdB", path("Root", "Other"));
        when(src.getCode()).thenReturn("C1");
        MigrationProduct stored = product("p1", "ProdB", chain("Treasury", "Root", "Mid", "Other"));
        when(stored.getCode()).thenReturn("C2");
        hierarchyHits.add(stored);

        ReconReport r = run("Treasury", src);

        assertEquals("ProdB", only(r, DiffType.PATH_MISMATCH).key());
        assertEquals("code", only(r, DiffType.PRODUCT_FIELD_MISMATCH).field());
    }

    // =====================================================================================
    // node fields
    // =====================================================================================

    @Test
    void nodeFields_everyFieldIngestionSetsIsCompared() {
        Node root = chain("Treasury", "Root");
        when(root.getNodeDescription()).thenReturn("x");
        when(root.getLevelNumber()).thenReturn(9L);
        when(root.getNodeStatus()).thenReturn("OTHER");
        when(root.getHierarchyName()).thenReturn("Other");
        when(root.getHierarchyType()).thenReturn("OTHER");
        when(root.getHierarchyPath()).thenReturn("Q");
        hierarchyHits.add(product("p1", "ProdA", root));

        ReconReport r = run("Treasury", source("ProdA", path("Root")));

        List<ReconDifference> diffs = ofType(r, DiffType.NODE_FIELD_MISMATCH);
        assertEquals(r.differences().size(), diffs.size());
        assertEquals(new HashSet<>(Arrays.asList("nodeDescription", "levelNumber", "nodeStatus",
                        "hierarchyName", "hierarchyType", "hierarchyPath")),
                diffs.stream().map(ReconDifference::field).collect(Collectors.toSet()));
        assertTrue(diffs.stream().allMatch(d -> d.key().equals("Root")));

        ReconDifference level = diffs.stream().filter(d -> d.field().equals("levelNumber")).findFirst().get();
        assertEquals("1", level.sourceValue());
        assertEquals("9", level.targetValue());
        ReconDifference description = diffs.stream().filter(d -> d.field().equals("nodeDescription")).findFirst().get();
        assertEquals("Root", description.sourceValue());
        assertEquals("x", description.targetValue());
    }

    @Test
    void nodeFields_aNodeSharedByTwoProductsIsReportedOnce_andDeeperNodesUseTheirFullPath() {
        Node leafA = chain("Treasury", "Root", "Mid");
        Node leafB = chain("Treasury", "Root", "Other");
        when(leafA.getParentNode().getNodeDescription()).thenReturn("X");   // Root, wrong in both product chains
        when(leafB.getParentNode().getNodeDescription()).thenReturn("X");
        when(leafB.getHierarchyPath()).thenReturn("wrong");                  // Root/Other
        hierarchyHits.add(product("p1", "ProdA", leafA));
        hierarchyHits.add(product("p2", "ProdB", leafB));

        ReconReport r = run("Treasury", source("ProdA", path("Root", "Mid")), source("ProdB", path("Root", "Other")));

        List<ReconDifference> diffs = ofType(r, DiffType.NODE_FIELD_MISMATCH);
        assertEquals(2, diffs.size(), diffs.toString());
        ReconDifference shared = diffs.stream().filter(d -> d.key().equals("Root")).findFirst().get();
        assertEquals("nodeDescription", shared.field());
        ReconDifference deeper = diffs.stream().filter(d -> d.key().equals("Root/Other")).findFirst().get();
        assertEquals("hierarchyPath", deeper.field());
        assertEquals("Root/Other", deeper.sourceValue());
        assertEquals("wrong", deeper.targetValue());
    }

    @Test
    void nodeFields_areNotComparedWhenThePathsDiffer() {
        Node leaf = chain("Treasury", "Root", "Mid", "Other");
        when(leaf.getParentNode().getParentNode().getNodeDescription()).thenReturn("X");   // would differ if compared
        hierarchyHits.add(product("p1", "ProdB", leaf));

        ReconReport r = run("Treasury", source("ProdB", path("Root", "Other")));

        assertEquals("ProdB", only(r, DiffType.PATH_MISMATCH).key());
        assertTrue(ofType(r, DiffType.NODE_FIELD_MISMATCH).isEmpty());
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

    /** A bare node mock (identity, name, hierarchy, parent); id == null means "no identifier". */
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

    /** The remaining fields ingestion sets on a node, filled so they AGREE with the on-prem level. */
    private static void describe(Node n, String name, long level, String path) {
        when(n.getNodeDescription()).thenReturn(name);
        when(n.getLevelNumber()).thenReturn(level);
        when(n.getNodeStatus()).thenReturn(ACTIVE_STATUS);
        when(n.getHierarchyType()).thenReturn(MIGRATION_PRODUCT_TYPE);
        when(n.getHierarchyPath()).thenReturn(path);
    }

    /** Builds root -> ... -> leaf (level numbers 1..n, cumulative paths) and returns the LEAF. */
    private Node chain(String hierarchy, String... names) {
        Node parent = null;
        StringBuilder path = new StringBuilder();
        for (int i = 0; i < names.length; i++) {
            path.append(i > 0 ? "/" : "").append(names[i]);
            parent = node("n" + (++seq), hierarchy, names[i], parent);
            describe(parent, names[i], i + 1, path.toString());
        }
        return parent;
    }

    /** Stored nodes that mirror the given on-prem levels exactly (for cases where levels are not 1..n). */
    private Node targetOf(NodeLevel... levels) {
        Node parent = null;
        for (NodeLevel l : levels) {
            parent = node("n" + (++seq), l.getHierarchyName(), l.getNodeName(), parent);
            describe(parent, l.getNodeName(), l.getLevel().longValue(), l.getNodePath());
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

    /** TODO: NodeLevel.getLevel() is assumed to return an Integer here; use whatever type it really returns. */
    private static NodeLevel levelAt(String hierarchy, String name, String sourceRef, int level, String nodePath) {
        NodeLevel l = mock(NodeLevel.class);
        when(l.getHierarchyName()).thenReturn(hierarchy);
        when(l.getNodeName()).thenReturn(name);
        when(l.getSourceNodeId()).thenReturn(sourceRef);
        when(l.getLevel()).thenReturn(level);
        when(l.getNodePath()).thenReturn(nodePath);
        return l;
    }

    /** On-prem levels for names root -> leaf: level numbers 1..n and cumulative paths, hierarchy "Treasury". */
    private static NodeLevel[] path(String... names) {
        NodeLevel[] levels = new NodeLevel[names.length];
        StringBuilder path = new StringBuilder();
        for (int i = 0; i < names.length; i++) {
            path.append(i > 0 ? "/" : "").append(names[i]);
            levels[i] = levelAt("Treasury", names[i], null, i + 1, path.toString());
        }
        return levels;
    }

    private static ProductHierarchyData source(String productName, NodeLevel... levels) {
        ProductHierarchyData p = mock(ProductHierarchyData.class);
        when(p.getProductName()).thenReturn(productName);
        List<NodeLevel> list = levels == null ? null : Arrays.asList(levels);
        when(p.getHierarchyLevels()).thenReturn(list);
        return p;
    }
}
