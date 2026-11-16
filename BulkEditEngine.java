package com.db.rds.platform.xrds.user.bff.bulkedit;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;

/**
 * Complete bulk edit engine for any entity and any allowed field.
 *
 * SUBMIT SIDE (maker)    grid / upload / status change -> change sets -> per-record pipeline -> work case
 * APPROVAL SIDE (checker) stale check at approval (S3/S4): reload, rebase, conflict check, re-run rules, conditional save
 *
 * Stale-copy windows covered
 *   S1 grid or file loaded, then user submits   -> version check on submit (REJECT or FIELD_LEVEL)
 *   S2 two submits for the same record at once  -> per-record lock with owner token + open-work-case check + unique-index backstop
 *   S3 submit, then checker approves later      -> approve() replays the change set on the CURRENT record, conflict check, conditional save
 *   S4 work case left open too long             -> S3 plus optional expiry (Config.maxApprovalAge)
 *
 * The engine knows nothing about Mongo or Spring. Everything external comes in through the ports in PART 2.
 * Wire them to: EntityService/EntityFieldsService, EntityPatchApplier, FieldPathValidator, MongoPrimeDataService,
 * SupportDataStore (locks, idempotency, templates, jobs), RecordProcessService and MakerCheckerApplyChangesTaskProcessor.
 *
 * Contents
 *   PART 1  vocabulary          PART 5  entry points (grid, upload, status change, templates, async jobs)
 *   PART 2  ports               PART 6  per-record submit pipeline
 *   PART 3  reference lock      PART 7  stale check (S1), shared by submit and approval
 *   PART 4  engine setup        PART 8  approval side (S3/S4)
 *                               PART 9  helpers (result file, no-op comparison)
 */
public class BulkEditEngine<T> {

    private static final System.Logger log = System.getLogger(BulkEditEngine.class.getName());

    /** In an upload cell, this text means "clear the field". A blank cell means "no change". */
    public static final String CLEAR_TOKEN = "{CLEAR}";
    /** Status moves only through the status-change source, never through a generic field edit. */
    public static final String STATUS_PATH = "status";

    // =====================================================================
    // PART 1: vocabulary
    // =====================================================================

    public enum Mode { VALIDATE, SUBMIT }
    public enum Source { GRID, UPLOAD, STATUS_CHANGE, FORM }
    public enum StalePolicy { REJECT, FIELD_LEVEL }
    /** REBASE: replay the change set on the current record at approval. STRICT: any version drift rejects. */
    public enum ApprovalPolicy { REBASE, STRICT }
    public enum Op { SET, CLEAR }
    public enum Outcome { SUBMITTED, VALIDATED, SKIPPED, FAILED }
    public enum ReasonCode {
        NOT_FOUND, STALE, FIELD_CONFLICT, PENDING_APPROVAL, NOT_EDITABLE,
        VALIDATION, NOT_ENTITLED, LOCKED, ERROR
    }
    public enum ApprovalOutcome { LEGACY_PATH, APPLIED, REBASED_AND_APPLIED, NO_CHANGE, CONFLICT, EXPIRED, FAILED }

    /** One field change. "expected" is the old value the user saw (optional, checked per field). */
    public record FieldChange(String path, Op op, Object value, boolean expectedGiven, Object expected) {
        public static FieldChange set(String path, Object value) {
            return new FieldChange(path, Op.SET, value, false, null);
        }
        public static FieldChange clear(String path) {
            return new FieldChange(path, Op.CLEAR, null, false, null);
        }
        public FieldChange expecting(Object oldValue) {
            return new FieldChange(path, op, value, true, oldValue);
        }
        Object target() {
            return op == Op.CLEAR ? null : value;
        }
    }

    /** The change set for ONE record: what the user saw (baseVersion) and what they want changed. */
    public record ItemEdit(String id, Integer rowNumber, Long baseVersion, List<FieldChange> changes) {}

    /** The one contract every entry route is translated into. */
    public record BulkEditRequest(String bulkRequestId, Source source, Mode mode,
                                  StalePolicy stalePolicy, String comment, List<ItemEdit> items) {}

    public record FieldError(String path, String message) {}

    public record RowResult(String id, Integer rowNumber, Outcome outcome, ReasonCode reasonCode,
                            String message, List<FieldError> fieldErrors) {}

    public record BulkEditResponse(String bulkRequestId, int requested, int submitted, int validated,
                                   int skipped, int failed, List<RowResult> results) {
        /** Existing rule: 200 if nothing failed, 207 if some failed. A bad request (400) is an IllegalArgumentException. */
        public int httpStatus() {
            return failed == 0 ? 200 : 207;
        }
    }

    // ---- request shapes for the three entry routes ----

    public record GridItem(String id, Long baseVersion) {}
    /** POST /bulk-edit */
    public record GridRequest(String bulkRequestId, List<GridItem> items, List<FieldChange> changes,
                              String comment, Mode mode, StalePolicy stalePolicy) {}

    public record UploadRow(int rowNumber, String id, Long version, Map<String, Object> cells) {}
    /** POST /bulk-edit/upload (rows are already parsed from the xlsx by the controller). */
    public record UploadRequest(String bulkRequestId, String templateId, List<UploadRow> rows, String comment,
                                Mode mode, StalePolicy stalePolicy, boolean applyToLatest) {}

    /** Status change goes through the same contract, with source = STATUS_CHANGE. */
    public record StatusChangeRequest(String bulkRequestId, List<GridItem> items, String targetStatus,
                                      String comment, Mode mode) {}

    // ---- template (download side of the upload flow) ----

    public record TemplateRow(String id, Long version, Map<String, Object> values) {}
    public record Template(String templateId, List<String> fields, List<TemplateRow> rows) {}

    // ---- approval side ----

    /** Mirrors the new optional fields on MakerCheckerTaskProperties. changeSet == null means "old single-record flow". */
    public record ApprovalTask(String workCaseId, String entityId, String maker, Long baseVersion,
                               List<FieldChange> changeSet, String bulkRequestId, Source source,
                               StalePolicy stalePolicy, Instant createdAt, String comment) {}

    public record ApprovalResult(ApprovalOutcome outcome, ReasonCode reason, String message,
                                 List<FieldError> fieldErrors) {}

    // ---- async job support ----

    public interface ProgressListener {
        void onProgress(int done, int total);
    }

    // =====================================================================
    // PART 2: ports (what the engine needs from the outside world)
    // =====================================================================

    /** Read access to the stored record, its history, and the conditional save used at approval. */
    public interface EntityAccess<T> {
        Optional<T> load(String id);
        long versionOf(T entity);
        /** Snapshot at an older version from the history collection. Empty if not kept. */
        Optional<T> loadAtVersion(String id, long version);
        /**
         * Replace the record only if its stored version still equals expectedVersion
         * (Mongo: replaceOne with filter {_id, idVersion: expectedVersion}). The usual version bump
         * (BaseOrchestrator.updatePrimaryIdentifier) must still happen. Return false if no row matched.
         */
        boolean saveIfVersion(T entity, long expectedVersion);
    }

    /** Wraps EntityPatchApplier. Must work on a deep copy and never touch the stored record. */
    public interface EntityPatcher<T> {
        Object read(T entity, String path);
        T apply(T entity, List<FieldChange> changes);
    }

    /** Wraps FieldPathValidator. Allows STATUS_PATH only for Source.STATUS_CHANGE. */
    public interface PathValidator {
        List<FieldError> validate(List<FieldChange> changes, Source source);
        List<FieldError> validateEditable(List<String> paths, Source source);
    }

    /** Domain opt-in hooks, from EntityService. Safe defaults so nothing changes until a domain opts in. */
    public interface DomainHooks<T> {
        boolean isEntitled(String user, T stored);
        boolean canBulkEdit(T stored);
        /** Dry-run rules at submit time (also validates status transitions for STATUS_CHANGE). */
        List<FieldError> validateProposed(T proposed);
        /** Re-run of the checker orchestrator (UI_SAVE) on the rebased record at approval. */
        List<FieldError> runCheckerRules(T rebased);
        void stampAudit(T copy, String user, String comment, Instant now);
        default ApprovalPolicy approvalPolicy() { return ApprovalPolicy.REBASE; }
        default boolean canApplyToLatest(String user) { return false; }
        /** Publish events, write history, etc. Called ONLY after the conditional save succeeded. */
        default void afterSaved(T saved, String checker) {}
    }

    /** Short per-record lock with an owner token, so a slow holder can never free someone else's lock. */
    public interface LockService {
        Optional<Lock> tryAcquire(String key, Duration ttl);
        boolean renew(Lock lock, Duration ttl);
        void release(Lock lock);
    }
    public record Lock(String key, String ownerToken) {}

    /** Wraps RecordProcessService.raiseWorkCase plus the workflow store. */
    public interface WorkCaseGateway<T> {
        boolean hasOpenWorkCase(String entityId);
        /** Must throw OpenWorkCaseExistsException if the unique partial index rejects a second Outstanding case. */
        void raise(WorkCaseDraft<T> draft);
        void addComment(String workCaseId, String comment);
    }
    public record WorkCaseDraft<T>(String entityId, T proposed, ItemEdit edit, String bulkRequestId, Source source,
                                   StalePolicy stalePolicy, String maker, String comment, Instant createdAt) {}
    public static class OpenWorkCaseExistsException extends RuntimeException {
        public OpenWorkCaseExistsException(String message) { super(message); }
    }

    /** Makes a retried request harmless: one (bulkRequestId, entityId) pair is processed once. */
    public interface IdempotencyStore {
        /** true if this pair was newly claimed, false if it already existed. Back with a unique index. */
        boolean claim(String bulkRequestId, String entityId);
        void release(String bulkRequestId, String entityId);
    }

    /** Remembers the original cell values of a generated template, keyed by template id (with an expiry). */
    public interface TemplateStore {
        void save(String templateId, Map<String, Map<String, Object>> originalsByEntityId);
        /** Empty map if the template is unknown or expired. */
        Map<String, Map<String, Object>> load(String templateId);
    }

    /** Progress and results for GET /bulk-edit/jobs/{id} and /jobs/{id}/results. */
    public interface JobStore {
        void create(String jobId, String user, int total);
        void progress(String jobId, int done, int total);
        void complete(String jobId, BulkEditResponse response, String resultCsv);
        void fail(String jobId, String message);
    }

    /** Optional: count STALE / FIELD_CONFLICT / LOCKED rates to see whether feeds fight the users. */
    public interface Metrics {
        void rowOutcome(Outcome outcome, ReasonCode reason);
    }

    public record Config(String domain, Duration lockTtl, int maxItems, int asyncThreshold,
                         Duration maxApprovalAge, int approvalRetries) {
        public static Config defaults(String domain) {
            return new Config(domain, Duration.ofSeconds(60), 500, 200, Duration.ofDays(7), 3);
        }
    }

    /** jobs, executor and metrics may be null when not needed. */
    public record Ports<T>(EntityAccess<T> access, EntityPatcher<T> patcher, PathValidator pathValidator,
                           DomainHooks<T> hooks, LockService locks, WorkCaseGateway<T> workCases,
                           IdempotencyStore idempotency, TemplateStore templates,
                           JobStore jobs, ExecutorService executor, Metrics metrics) {}

    // =====================================================================
    // PART 3: reference lock (owner token + expiresAt takeover)
    // =====================================================================

    /**
     * What the SupportDataStore needs to offer for locks. All four must be atomic single-document operations:
     *  insertIfAbsent  insert {_id: key, owner, expiresAt}; false on duplicate key
     *  takeOverIfExpired findOneAndUpdate({_id: key, expiresAt < now}, {$set: owner, expiresAt})
     *  extendIfOwner   updateOne({_id: key, owner}, {$set: expiresAt})
     *  deleteIfOwner   deleteOne({_id: key, owner})
     * Do not rely on a TTL index alone: it deletes lazily (about once a minute), so expiresAt is checked explicitly.
     */
    public interface LockStore {
        boolean insertIfAbsent(String key, String owner, Instant expiresAt);
        boolean takeOverIfExpired(String key, String newOwner, Instant now, Instant expiresAt);
        boolean extendIfOwner(String key, String owner, Instant expiresAt);
        void deleteIfOwner(String key, String owner);
    }

    public static class TokenLockService implements LockService {
        private final LockStore store;
        private final Clock clock;

        public TokenLockService(LockStore store, Clock clock) {
            this.store = store;
            this.clock = clock;
        }

        @Override
        public Optional<Lock> tryAcquire(String key, Duration ttl) {
            String owner = UUID.randomUUID().toString();
            Instant now = clock.instant();
            Instant expires = now.plus(ttl);
            if (store.insertIfAbsent(key, owner, expires) || store.takeOverIfExpired(key, owner, now, expires)) {
                return Optional.of(new Lock(key, owner));
            }
            return Optional.empty();
        }

        @Override
        public boolean renew(Lock lock, Duration ttl) {
            return store.extendIfOwner(lock.key(), lock.ownerToken(), clock.instant().plus(ttl));
        }

        @Override
        public void release(Lock lock) {
            store.deleteIfOwner(lock.key(), lock.ownerToken());
        }
    }

    // =====================================================================
    // PART 4: engine setup
    // =====================================================================

    private final Config config;
    private final Clock clock;
    private final EntityAccess<T> access;
    private final EntityPatcher<T> patcher;
    private final PathValidator pathValidator;
    private final DomainHooks<T> hooks;
    private final LockService locks;
    private final WorkCaseGateway<T> workCases;
    private final IdempotencyStore idempotency;
    private final TemplateStore templates;
    private final JobStore jobs;
    private final ExecutorService executor;
    private final Metrics metrics;

    public BulkEditEngine(Config config, Ports<T> ports, Clock clock) {
        this.config = config;
        this.clock = clock;
        this.access = ports.access();
        this.patcher = ports.patcher();
        this.pathValidator = ports.pathValidator();
        this.hooks = ports.hooks();
        this.locks = ports.locks();
        this.workCases = ports.workCases();
        this.idempotency = ports.idempotency();
        this.templates = ports.templates();
        this.jobs = ports.jobs();
        this.executor = ports.executor();
        this.metrics = ports.metrics();
    }

    // =====================================================================
    // PART 5: entry points
    // =====================================================================

    /** GET /bulk-edit/template?fields= : records the original values so uploads can detect stale cells per field. */
    public Template prepareTemplate(List<String> ids, List<String> fields, String user) {
        List<FieldError> bad = pathValidator.validateEditable(fields, Source.UPLOAD);
        if (!bad.isEmpty()) {
            throw new IllegalArgumentException("Fields cannot be bulk edited: " + bad);
        }
        String templateId = UUID.randomUUID().toString();
        List<TemplateRow> rows = new ArrayList<>();
        Map<String, Map<String, Object>> originals = new LinkedHashMap<>();
        for (String id : ids) {
            Optional<T> loaded = access.load(id);
            if (loaded.isEmpty() || !hooks.isEntitled(user, loaded.get()) || !hooks.canBulkEdit(loaded.get())) {
                continue; // not in the template, so it cannot be edited through it
            }
            T entity = loaded.get();
            Map<String, Object> values = new LinkedHashMap<>();
            for (String path : fields) {
                values.put(path, patcher.read(entity, path));
            }
            originals.put(id, values);
            rows.add(new TemplateRow(id, access.versionOf(entity), values));
        }
        templates.save(templateId, originals);
        return new Template(templateId, fields, rows);
    }

    /** POST /bulk-edit : shared changes applied to every selected row, each with the version the grid showed. */
    public BulkEditResponse executeGrid(GridRequest g, String user) {
        if (g == null || g.items() == null || g.changes() == null || g.changes().isEmpty()) {
            throw new IllegalArgumentException("items and changes are required");
        }
        List<ItemEdit> items = new ArrayList<>();
        for (GridItem gi : g.items()) {
            items.add(new ItemEdit(gi.id(), null, gi.baseVersion(), g.changes()));
        }
        return execute(new BulkEditRequest(g.bulkRequestId(), Source.GRID, g.mode(), g.stalePolicy(),
                g.comment(), items), user, null);
    }

    /** Status change: same contract, source = STATUS_CHANGE, a single "status" SET. */
    public BulkEditResponse executeStatusChange(StatusChangeRequest s, String user) {
        if (s == null || s.items() == null || s.targetStatus() == null || s.targetStatus().isBlank()) {
            throw new IllegalArgumentException("items and targetStatus are required");
        }
        List<FieldChange> changes = List.of(FieldChange.set(STATUS_PATH, s.targetStatus()));
        List<ItemEdit> items = new ArrayList<>();
        for (GridItem gi : s.items()) {
            items.add(new ItemEdit(gi.id(), null, gi.baseVersion(), changes));
        }
        return execute(new BulkEditRequest(s.bulkRequestId(), Source.STATUS_CHANGE, s.mode(), null,
                s.comment(), items), user, null);
    }

    public BulkEditResponse executeUpload(UploadRequest up, String user) {
        return executeUpload(up, user, null);
    }

    /** POST /bulk-edit/upload : turns rows into change sets, rejects unusable rows, runs the rest. */
    public BulkEditResponse executeUpload(UploadRequest up, String user, ProgressListener listener) {
        if (up == null || up.rows() == null || up.rows().isEmpty()) {
            throw new IllegalArgumentException("The file has no rows");
        }
        String bulkId = up.bulkRequestId() != null ? up.bulkRequestId() : UUID.randomUUID().toString();
        Map<String, Map<String, Object>> originals =
                up.templateId() == null ? Map.of() : templates.load(up.templateId());
        boolean applyLatest = up.applyToLatest() && hooks.canApplyToLatest(user);

        List<RowResult> pre = new ArrayList<>();      // rows decided before the engine even starts
        List<ItemEdit> items = new ArrayList<>();
        Set<String> seen = new HashSet<>();

        for (UploadRow row : up.rows()) {
            if (row.id() == null || row.id().isBlank()) {
                pre.add(track(rowFail(null, row.rowNumber(), ReasonCode.VALIDATION, "Row has no id")));
                continue;
            }
            if (!seen.add(row.id())) {
                pre.add(track(rowFail(row.id(), row.rowNumber(), ReasonCode.VALIDATION, "Duplicate id in file")));
                continue;
            }
            Long baseVersion = row.version();
            if (baseVersion == null) {
                if (up.applyToLatest() && !applyLatest) {
                    pre.add(track(rowFail(row.id(), row.rowNumber(), ReasonCode.NOT_ENTITLED,
                            "You may not apply uploads to the latest version")));
                    continue;
                }
                if (!applyLatest) {
                    pre.add(track(rowFail(row.id(), row.rowNumber(), ReasonCode.STALE,
                            "Version is missing, download a fresh template")));
                    continue;
                }
            }
            List<FieldChange> changes = changesFromCells(row, originals.getOrDefault(row.id(), Map.of()));
            if (changes.isEmpty()) {
                pre.add(track(new RowResult(row.id(), row.rowNumber(), Outcome.SKIPPED, null,
                        "No edits in this row", null)));
                continue;
            }
            items.add(new ItemEdit(row.id(), row.rowNumber(), baseVersion, changes));
        }

        List<RowResult> all = new ArrayList<>(pre);
        if (!items.isEmpty()) {
            BulkEditResponse core = execute(new BulkEditRequest(bulkId, Source.UPLOAD, up.mode(),
                    up.stalePolicy(), up.comment(), items), user, listener);
            all.addAll(core.results());
        }
        all.sort(Comparator.comparing(RowResult::rowNumber, Comparator.nullsLast(Comparator.naturalOrder())));
        return summarize(bulkId, all);
    }

    /** A blank cell is no change, a cell equal to the original is no change, {CLEAR} clears. */
    private List<FieldChange> changesFromCells(UploadRow row, Map<String, Object> originals) {
        List<FieldChange> changes = new ArrayList<>();
        for (Map.Entry<String, Object> cell : row.cells().entrySet()) {
            String path = cell.getKey();
            Object value = cell.getValue();
            if (value == null || (value instanceof String s && s.isBlank())) continue;
            boolean hasOriginal = originals.containsKey(path);
            if (hasOriginal && sameValue(value, originals.get(path))) continue; // user did not touch this cell
            FieldChange change = CLEAR_TOKEN.equals(value) ? FieldChange.clear(path) : FieldChange.set(path, value);
            changes.add(hasOriginal ? change.expecting(originals.get(path)) : change);
        }
        return changes;
    }

    public boolean shouldRunAsync(int rowCount) {
        return rowCount > config.asyncThreshold();
    }

    /** Large uploads: returns a jobId at once; progress and the result file come from the JobStore. */
    public String submitUploadAsync(UploadRequest up, String user) {
        if (jobs == null || executor == null) {
            throw new IllegalStateException("Async jobs are not configured");
        }
        String jobId = UUID.randomUUID().toString();
        UploadRequest withId = new UploadRequest(up.bulkRequestId() != null ? up.bulkRequestId() : jobId,
                up.templateId(), up.rows(), up.comment(), up.mode(), up.stalePolicy(), up.applyToLatest());
        jobs.create(jobId, user, up.rows().size());
        executor.submit(() -> {
            try {
                BulkEditResponse response = executeUpload(withId, user, (done, total) -> jobs.progress(jobId, done, total));
                jobs.complete(jobId, response, toCsv(response));
            } catch (RuntimeException e) {
                log.log(System.Logger.Level.ERROR, "Bulk edit job " + jobId + " failed", e);
                jobs.fail(jobId, e.getMessage());
            }
        });
        return jobId;
    }

    /** The core: every route ends up here. */
    public BulkEditResponse execute(BulkEditRequest request, String user, ProgressListener listener) {
        validateRequest(request);
        BulkEditRequest req = request.bulkRequestId() != null ? request
                : new BulkEditRequest(UUID.randomUUID().toString(), request.source(), request.mode(),
                request.stalePolicy(), request.comment(), request.items());
        StalePolicy policy = effectivePolicy(req);

        List<RowResult> results = new ArrayList<>(req.items().size());
        int done = 0;
        for (ItemEdit item : req.items()) {              // one record never stops the others
            results.add(processOne(req, policy, item, user));
            done++;
            if (listener != null) listener.onProgress(done, req.items().size());
        }
        return summarize(req.bulkRequestId(), results);
    }

    private void validateRequest(BulkEditRequest r) {
        if (r == null || r.items() == null || r.items().isEmpty()) {
            throw new IllegalArgumentException("No records to edit");
        }
        if (r.items().size() > config.maxItems()) {
            throw new IllegalArgumentException("Too many records (max " + config.maxItems() + ")");
        }
        if (r.mode() == null || r.source() == null) {
            throw new IllegalArgumentException("mode and source are required");
        }
        Set<String> seen = new HashSet<>();
        for (ItemEdit i : r.items()) {
            if (i.id() == null || i.id().isBlank()) {
                throw new IllegalArgumentException("Every item needs an id");
            }
            if (!seen.add(i.id())) {
                throw new IllegalArgumentException("Duplicate id in request: " + i.id());
            }
            if (i.changes() == null || i.changes().isEmpty()) {
                throw new IllegalArgumentException("No changes for " + i.id());
            }
        }
    }

    /** Defaults from 4.1: uploads reject stale rows, grid and status change merge field by field. */
    private StalePolicy effectivePolicy(BulkEditRequest r) {
        if (r.stalePolicy() != null) return r.stalePolicy();
        return r.source() == Source.UPLOAD ? StalePolicy.REJECT : StalePolicy.FIELD_LEVEL;
    }

    // =====================================================================
    // PART 6: the per-record submit pipeline
    // =====================================================================

    private RowResult processOne(BulkEditRequest req, StalePolicy policy, ItemEdit item, String user) {
        return track(processOneInternal(req, policy, item, user));
    }

    private RowResult processOneInternal(BulkEditRequest req, StalePolicy policy, ItemEdit item, String user) {
        Lock lock = null;
        try {
            // 0. cheap checks first, before any lock is taken
            List<FieldError> pathErrors = pathValidator.validate(item.changes(), req.source());
            if (!pathErrors.isEmpty()) {
                throw fail(ReasonCode.VALIDATION, "Some fields cannot be edited or have invalid values", pathErrors);
            }

            // 1. lock (S2)
            lock = locks.tryAcquire(config.domain() + ":" + item.id(), config.lockTtl())
                    .orElseThrow(() -> fail(ReasonCode.LOCKED, "Another bulk edit is working on this record", null));

            // 2. load the current record
            T current = access.load(item.id())
                    .orElseThrow(() -> fail(ReasonCode.NOT_FOUND, "Record not found", null));

            // 3. entitlement and editable state
            if (!hooks.isEntitled(user, current)) {
                throw fail(ReasonCode.NOT_ENTITLED, "You are not entitled to edit this record", null);
            }
            if (!hooks.canBulkEdit(current)) {
                throw fail(ReasonCode.NOT_EDITABLE, "Record is in a state that cannot be bulk edited", null);
            }

            // 4. drop no-ops and run the S1 stale check; nothing left means SKIPPED
            List<FieldChange> effective =
                    resolveEffectiveChanges(policy, item.id(), item.baseVersion(), item.changes(), current);
            if (effective.isEmpty()) {
                return new RowResult(item.id(), item.rowNumber(), Outcome.SKIPPED, null,
                        "No difference from the current record", null);
            }

            // 5. is a work case already open? (checked inside the lock)
            if (workCases.hasOpenWorkCase(item.id())) {
                throw fail(ReasonCode.PENDING_APPROVAL, "A change for this record is already awaiting approval", null);
            }

            // 6. apply on a COPY and stamp audit
            T proposed = patcher.apply(current, effective);
            hooks.stampAudit(proposed, user, req.comment(), clock.instant());

            // 7. domain rules, dry run
            List<FieldError> ruleErrors = hooks.validateProposed(proposed);
            if (!ruleErrors.isEmpty()) {
                throw fail(ReasonCode.VALIDATION, "Validation failed", ruleErrors);
            }

            // 8. VALIDATE stops here
            if (req.mode() == Mode.VALIDATE) {
                return new RowResult(item.id(), item.rowNumber(), Outcome.VALIDATED, null,
                        "Valid, ready to submit", null);
            }

            // 8b. SUBMIT: the lock must still be ours, the pair must not have been processed before
            if (!locks.renew(lock, config.lockTtl())) {
                throw fail(ReasonCode.LOCKED, "The lock expired while processing, please retry", null);
            }
            if (!idempotency.claim(req.bulkRequestId(), item.id())) {
                return new RowResult(item.id(), item.rowNumber(), Outcome.SKIPPED, null,
                        "Already processed in this bulk request", null);
            }
            boolean raised = false;
            try {
                workCases.raise(new WorkCaseDraft<>(item.id(), proposed,
                        new ItemEdit(item.id(), item.rowNumber(), item.baseVersion(), effective),
                        req.bulkRequestId(), req.source(), policy, user, req.comment(), clock.instant()));
                raised = true;
            } catch (OpenWorkCaseExistsException e) {    // backstop: the unique partial index said no
                throw fail(ReasonCode.PENDING_APPROVAL, "A change for this record is already awaiting approval", null);
            } finally {
                if (!raised) idempotency.release(req.bulkRequestId(), item.id());
            }
            return new RowResult(item.id(), item.rowNumber(), Outcome.SUBMITTED, null,
                    "Submitted for approval", null);

        } catch (RowFailure f) {
            return new RowResult(item.id(), item.rowNumber(), Outcome.FAILED, f.reason, f.getMessage(), f.fieldErrors);
        } catch (RuntimeException e) {
            log.log(System.Logger.Level.ERROR, "Bulk edit failed for " + item.id(), e);
            return new RowResult(item.id(), item.rowNumber(), Outcome.FAILED, ReasonCode.ERROR, "Unexpected error", null);
        } finally {
            releaseQuietly(lock);
        }
    }

    // =====================================================================
    // PART 7: no-ops and the stale check (S1), shared by submit and approval
    // =====================================================================

    /**
     * 1. Drop fields whose current value already equals the new value (no-ops).
     * 2. No baseVersion, or it matches the current version: fine.
     * 3. Version differs + REJECT: STALE.
     * 4. Version differs + FIELD_LEVEL: only the edited fields can conflict. Compare the current value with what
     *    the user saw (expected, or the value at baseVersion from history). A mismatch is a FIELD_CONFLICT.
     * Returns the changes that still need to be applied. Throws RowFailure for STALE / FIELD_CONFLICT.
     */
    private List<FieldChange> resolveEffectiveChanges(StalePolicy policy, String id, Long base,
                                                      List<FieldChange> changes, T current) {
        List<FieldChange> effective = new ArrayList<>();
        for (FieldChange c : changes) {
            if (!sameValue(patcher.read(current, c.path()), c.target())) {
                effective.add(c);
            }
        }

        long currentVersion = access.versionOf(current);
        if (base == null || base == currentVersion || effective.isEmpty()) {
            return effective;
        }

        if (policy == StalePolicy.REJECT) {
            throw fail(ReasonCode.STALE, String.format(
                    "Record changed since you loaded it (v%d -> v%d), download a fresh template", base, currentVersion), null);
        }

        T baseline = null;
        List<FieldError> conflicts = new ArrayList<>();
        for (FieldChange c : effective) {
            Object seenByUser;
            if (c.expectedGiven()) {
                seenByUser = c.expected();
            } else {
                if (baseline == null) {
                    baseline = access.loadAtVersion(id, base).orElseThrow(() -> fail(ReasonCode.STALE,
                            "Record changed since you loaded it and version " + base + " is no longer available", null));
                }
                seenByUser = patcher.read(baseline, c.path());
            }
            if (!sameValue(patcher.read(current, c.path()), seenByUser)) {
                conflicts.add(new FieldError(c.path(), "Changed by someone else since you loaded it"));
            }
        }
        if (!conflicts.isEmpty()) {
            throw fail(ReasonCode.FIELD_CONFLICT, "Some fields changed since you loaded the record", conflicts);
        }
        return effective;
    }

    // =====================================================================
    // PART 8: approval side (S3 and S4)
    // =====================================================================

    /**
     * Call from MakerCheckerApplyChangesTaskProcessor on approval.
     * LEGACY_PATH means "no change set: do exactly what the processor does today".
     * CONFLICT / EXPIRED / FAILED mean "approval fails": feed that into the existing failure handling
     * (comment on the work case, mark rejected/stale, maker resubmits). The reason and fieldErrors are for the comment.
     */
    public ApprovalResult approve(ApprovalTask task, String checker) {
        // 1. single-record form edits are unaffected
        if (task.changeSet() == null || task.changeSet().isEmpty()) {
            return new ApprovalResult(ApprovalOutcome.LEGACY_PATH, null, "No change set, legacy flow", null);
        }

        // 7 (S4). optional expiry
        if (config.maxApprovalAge() != null && task.createdAt() != null
                && clock.instant().isAfter(task.createdAt().plus(config.maxApprovalAge()))) {
            return new ApprovalResult(ApprovalOutcome.EXPIRED, ReasonCode.STALE,
                    "Work case is older than " + config.maxApprovalAge().toDays() + " days, the maker must resubmit", null);
        }

        for (int attempt = 1; attempt <= Math.max(1, config.approvalRetries()); attempt++) {
            // 2. load the current record
            Optional<T> loaded = access.load(task.entityId());
            if (loaded.isEmpty()) {
                return new ApprovalResult(ApprovalOutcome.FAILED, ReasonCode.NOT_FOUND, "Record not found", null);
            }
            T current = loaded.get();
            long currentVersion = access.versionOf(current);
            Long base = task.baseVersion();
            boolean drifted = base != null && base != currentVersion;

            // strict entities never rebase
            if (drifted && hooks.approvalPolicy() == ApprovalPolicy.STRICT) {
                return new ApprovalResult(ApprovalOutcome.CONFLICT, ReasonCode.STALE, String.format(
                        "Record changed since submission (v%d -> v%d), the maker must resubmit", base, currentVersion), null);
            }

            // 3 + 4. same version: apply. Different version: rebase with the field conflict check from 4.2
            List<FieldChange> effective;
            try {
                effective = resolveEffectiveChanges(task.stalePolicy(), task.entityId(), base, task.changeSet(), current);
            } catch (RowFailure f) {
                return new ApprovalResult(ApprovalOutcome.CONFLICT, f.reason, f.getMessage(), f.fieldErrors);
            }
            if (effective.isEmpty()) {
                return new ApprovalResult(ApprovalOutcome.NO_CHANGE, null,
                        "Record already has the requested values, nothing to save", null);
            }

            // 5. apply on a copy and re-run the checker rules on the rebased record
            T rebased = patcher.apply(current, effective);
            hooks.stampAudit(rebased, task.maker(), task.comment(), clock.instant());
            List<FieldError> errors = hooks.runCheckerRules(rebased);
            if (!errors.isEmpty()) {
                return new ApprovalResult(ApprovalOutcome.FAILED, ReasonCode.VALIDATION,
                        "Validation failed on the current record", errors);
            }

            // 6. save only if the version still matches; if not, someone slipped in, so load again
            if (access.saveIfVersion(rebased, currentVersion)) {
                if (drifted) {
                    workCases.addComment(task.workCaseId(), String.format(
                            "Rebased from v%d to v%d (fields applied: %s)", base, currentVersion, paths(effective)));
                }
                hooks.afterSaved(rebased, checker);   // events only after a successful save
                return new ApprovalResult(drifted ? ApprovalOutcome.REBASED_AND_APPLIED : ApprovalOutcome.APPLIED,
                        null, drifted ? "Rebased and saved" : "Saved", null);
            }
        }
        return new ApprovalResult(ApprovalOutcome.CONFLICT, ReasonCode.STALE,
                "The record kept changing during approval, the maker must resubmit", null);
    }

    // =====================================================================
    // PART 9: helpers
    // =====================================================================

    private BulkEditResponse summarize(String bulkRequestId, List<RowResult> results) {
        int submitted = 0, validated = 0, skipped = 0, failed = 0;
        for (RowResult r : results) {
            switch (r.outcome()) {
                case SUBMITTED -> submitted++;
                case VALIDATED -> validated++;
                case SKIPPED -> skipped++;
                case FAILED -> failed++;
            }
        }
        return new BulkEditResponse(bulkRequestId, results.size(), submitted, validated, skipped, failed, results);
    }

    private RowResult track(RowResult r) {
        if (metrics != null) metrics.rowOutcome(r.outcome(), r.reasonCode());
        return r;
    }

    private static RowResult rowFail(String id, Integer rowNumber, ReasonCode reason, String message) {
        return new RowResult(id, rowNumber, Outcome.FAILED, reason, message, null);
    }

    private void releaseQuietly(Lock lock) {
        if (lock == null) return;
        try {
            locks.release(lock);
        } catch (RuntimeException e) {
            log.log(System.Logger.Level.WARNING, "Could not release lock " + lock.key(), e);
        }
    }

    private static String paths(List<FieldChange> changes) {
        List<String> p = new ArrayList<>();
        for (FieldChange c : changes) p.add(c.path());
        return String.join(", ", p);
    }

    /** Numbers compare by value (1 == 1.0), everything else by equals. */
    static boolean sameValue(Object a, Object b) {
        if (a instanceof Number x && b instanceof Number y) {
            return new BigDecimal(x.toString()).compareTo(new BigDecimal(y.toString())) == 0;
        }
        return Objects.equals(a, b);
    }

    /** CSV for the downloadable result file. Cells that start like a formula are neutralised (spreadsheet injection). */
    public static String toCsv(BulkEditResponse response) {
        StringBuilder sb = new StringBuilder("rowNumber,id,outcome,reasonCode,message,fieldErrors\n");
        for (RowResult r : response.results()) {
            StringBuilder fe = new StringBuilder();
            if (r.fieldErrors() != null) {
                for (FieldError e : r.fieldErrors()) {
                    if (fe.length() > 0) fe.append("; ");
                    fe.append(e.path()).append(": ").append(e.message());
                }
            }
            sb.append(r.rowNumber() == null ? "" : r.rowNumber().toString()).append(',')
              .append(csv(r.id())).append(',')
              .append(r.outcome()).append(',')
              .append(r.reasonCode() == null ? "" : r.reasonCode().toString()).append(',')
              .append(csv(r.message())).append(',')
              .append(csv(fe.toString())).append('\n');
        }
        return sb.toString();
    }

    private static String csv(String value) {
        if (value == null) return "";
        String v = value;
        if (!v.isEmpty() && "=+-@\t\r".indexOf(v.charAt(0)) >= 0) {
            v = "'" + v;
        }
        return "\"" + v.replace("\"", "\"\"") + "\"";
    }

    private static RowFailure fail(ReasonCode reason, String message, List<FieldError> fieldErrors) {
        return new RowFailure(reason, message, fieldErrors);
    }

    /** Internal control-flow exception: a row failed for a known reason. No stack trace, it is cheap. */
    private static final class RowFailure extends RuntimeException {
        final ReasonCode reason;
        final List<FieldError> fieldErrors;

        RowFailure(ReasonCode reason, String message, List<FieldError> fieldErrors) {
            super(message, null, false, false);
            this.reason = reason;
            this.fieldErrors = fieldErrors;
        }
    }
}
