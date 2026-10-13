package e2e

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"time"

	"github.com/redhat-developer/rhdh-operator/tests/helper"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

const (
	postgresqlSourceImage   = "postgresql-15"
	postgresqlSourceVersion = "15."
	postgresqlTargetImage   = "postgresql-18"
	postgresqlTargetVersion = "18."
	postgresqlProofDatabase = "backstage_plugin_catalog"
	postgresqlProofValue    = "postgresql-15-to-18"
)

// postgresqlRestoreBootstrapConflict matches the only restore errors that are expected when a pg_dumpall dump is
// restored into a freshly initialized PostgreSQL instance, which already contains the bootstrap superuser and the
// default database. Keep this in sync with docs/postgresql-upgrade.md and the RHDH Helm upgrade CI
// (.ci/pipelines/lib/postgres.sh in redhat-developer/rhdh).
var postgresqlRestoreBootstrapConflict = regexp.MustCompile(`^ERROR:\s+(role|database) "postgres" already exists$`)

type postgresqlUpgradeState struct {
	dumpPath        string
	postgresPodUID  string
	pvcUID          string
	secretUID       string
	backstagePodUID string
	databaseNames   []string
	// tableRowCounts holds, per database, the exact row count of every user table before the dump.
	tableRowCounts map[string][]string
	// originalReplicas is the value of spec.deployment.patch.spec.replicas before the upgrade, or empty if unset.
	originalReplicas string
}

func logPostgresqlUpgradeCheckpoint(name string, fields ...string) {
	checkpoint := "postgresql-upgrade: " + name
	if len(fields) > 0 {
		checkpoint += " " + strings.Join(fields, " ")
	}
	fmt.Println(checkpoint)
}

func postgresqlUpgradeDiagnostics(namespace, crName string) string {
	crLabel := fmt.Sprintf("rhdh.redhat.com/app=backstage-%s", crName)
	postgresqlLabel := fmt.Sprintf("rhdh.redhat.com/app=%s", postgresqlStatefulSetName(crName))
	return fmt.Sprintf(
		"=== PostgreSQL upgrade diagnostics ===\n"+
			"=== Operator and operand logs ===\n%s"+
			"=== PostgreSQL logs ===\n%s"+
			"=== PostgreSQL description ===\n%s",
		fetchOperatorAndOperandLogs(managerPodLabel, namespace, crLabel),
		getPodLogs(namespace, "", postgresqlLabel),
		describePod(namespace, postgresqlLabel),
	)
}

func preparePostgresqlUpgrade(namespace, crName string) *postgresqlUpgradeState {
	state := &postgresqlUpgradeState{}
	postgresPod := waitForPostgresql(namespace, crName, postgresqlSourceImage, postgresqlSourceVersion)
	state.databaseNames = postgresqlDatabaseNames(namespace, postgresPod)
	Expect(state.databaseNames).To(ContainElement(postgresqlProofDatabase))

	By("seeding data that must survive the PostgreSQL upgrade")
	_, err := runPostgresqlSQL(namespace, postgresPod, postgresqlProofDatabase, `
CREATE TABLE public.rhdh_postgresql_upgrade_test (marker text PRIMARY KEY);
INSERT INTO public.rhdh_postgresql_upgrade_test VALUES ('`+postgresqlProofValue+`');`)
	Expect(err).NotTo(HaveOccurred())
	Expect(queryPostgresqlUpgradeProof(namespace, postgresPod)).To(Equal(postgresqlProofValue))

	state.postgresPodUID = resourceUID(namespace, "pod", postgresPod)
	state.pvcUID = resourceUID(namespace, "pvc", postgresqlPVCName(crName))
	state.secretUID = resourceUID(namespace, "secret", postgresqlSecretName(crName))
	state.backstagePodUID = readyPodUID(namespace, fmt.Sprintf("rhdh.redhat.com/app=backstage-%s", crName))
	logPostgresqlUpgradeCheckpoint("source-ready",
		fmt.Sprintf("pod=%s", postgresPod),
		fmt.Sprintf("podUID=%s", state.postgresPodUID),
		fmt.Sprintf("pvcUID=%s", state.pvcUID),
		fmt.Sprintf("secretUID=%s", state.secretUID),
		fmt.Sprintf("backstagePodUID=%s", state.backstagePodUID),
		fmt.Sprintf("databases=%d", len(state.databaseNames)),
		fmt.Sprintf("proofDatabase=%s", postgresqlProofDatabase),
		fmt.Sprintf("proofValue=%s", postgresqlProofValue),
	)

	By("stopping Backstage before taking the PostgreSQL dump")
	state.originalReplicas = backstagePatchReplicas(namespace, crName)
	setBackstageReplicas(namespace, crName, 0)
	waitForNoPods(namespace, fmt.Sprintf("rhdh.redhat.com/app=backstage-%s", crName))

	By("recording the exact row counts of every PostgreSQL 15 table")
	state.tableRowCounts = postgresqlTableRowCounts(namespace, postgresPod, state.databaseNames)
	Expect(hasBackstageOwnedRows(state.tableRowCounts[postgresqlProofDatabase])).To(BeTrue(),
		"expected Backstage-owned rows in %s, got %v", postgresqlProofDatabase, state.tableRowCounts[postgresqlProofDatabase])

	By("dumping all PostgreSQL 15 databases and roles")
	state.dumpPath = dumpPostgresql(namespace, postgresPod)

	// The source Operator ignores this annotation. The upgraded Operator honours it and applies the PostgreSQL 18
	// StatefulSet with zero replicas, so PostgreSQL 18 never starts against the PostgreSQL 15 data directory.
	By("idling the instance before upgrading the operator")
	setIdleAnnotation(namespace, crName, true)

	return state
}

func completePostgresqlUpgrade(namespace, crName string, state *postgresqlUpgradeState) {
	statefulSetName := postgresqlStatefulSetName(crName)

	By("waiting for the upgraded operator to select the PostgreSQL 18 image")
	Eventually(func(g Gomega) {
		out, err := helper.Run(exec.Command(helper.GetPlatformTool(), "-n", namespace, "get", "statefulset",
			statefulSetName, "-o", "jsonpath={.spec.template.spec.containers[0].image}"))
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(strings.TrimSpace(string(out))).To(ContainSubstring(postgresqlTargetImage))
	}, 5*time.Minute, 5*time.Second).Should(Succeed())

	By("waiting for the idled PostgreSQL StatefulSet to have no pods")
	Eventually(func(g Gomega) {
		out, err := helper.Run(exec.Command(helper.GetPlatformTool(), "-n", namespace, "get", "statefulset",
			statefulSetName, "-o", "jsonpath={.spec.replicas}"))
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(strings.TrimSpace(string(out))).To(Equal("0"))
	}, 5*time.Minute, 5*time.Second).Should(Succeed())
	waitForNoPods(namespace, fmt.Sprintf("rhdh.redhat.com/app=%s", statefulSetName))

	By("deleting the PostgreSQL 15 persistent volume claim")
	Expect(resourceUID(namespace, "pvc", postgresqlPVCName(crName))).To(Equal(state.pvcUID))
	deleteResource(namespace, "pvc", postgresqlPVCName(crName))
	Expect(resourceUID(namespace, "secret", postgresqlSecretName(crName))).To(Equal(state.secretUID))

	By("waking the instance to provision a fresh PostgreSQL 18 database while Backstage stays stopped")
	setIdleAnnotation(namespace, crName, false)

	postgresPod := waitForPostgresql(namespace, crName, postgresqlTargetImage, postgresqlTargetVersion)
	waitForNoPods(namespace, fmt.Sprintf("rhdh.redhat.com/app=backstage-%s", crName))
	targetPostgresPodUID := resourceUID(namespace, "pod", postgresPod)
	targetPVCUID := resourceUID(namespace, "pvc", postgresqlPVCName(crName))
	targetSecretUID := resourceUID(namespace, "secret", postgresqlSecretName(crName))
	Expect(targetPostgresPodUID).NotTo(Equal(state.postgresPodUID))
	Expect(targetPVCUID).NotTo(Equal(state.pvcUID))
	Expect(targetSecretUID).To(Equal(state.secretUID))
	logPostgresqlUpgradeCheckpoint("target-ready",
		fmt.Sprintf("pod=%s", postgresPod),
		fmt.Sprintf("previousPodUID=%s", state.postgresPodUID),
		fmt.Sprintf("podUID=%s", targetPostgresPodUID),
		fmt.Sprintf("previousPVCUID=%s", state.pvcUID),
		fmt.Sprintf("pvcUID=%s", targetPVCUID),
		fmt.Sprintf("secretUID=%s", targetSecretUID),
		"secretPreserved=true",
	)

	By("restoring the PostgreSQL 15 dump into PostgreSQL 18")
	restorePostgresql(namespace, postgresPod, state.dumpPath)
	restoredDatabaseNames := postgresqlDatabaseNames(namespace, postgresPod)
	Expect(restoredDatabaseNames).To(Equal(state.databaseNames))
	restoredRowCounts := postgresqlTableRowCounts(namespace, postgresPod, restoredDatabaseNames)
	Expect(restoredRowCounts).To(Equal(state.tableRowCounts))
	Expect(postgresqlCollationMismatches(namespace, postgresPod)).To(BeEmpty())
	Expect(queryPostgresqlUpgradeProof(namespace, postgresPod)).To(Equal(postgresqlProofValue))
	logPostgresqlUpgradeCheckpoint("restore-verified",
		fmt.Sprintf("databases=%d", len(restoredDatabaseNames)),
		fmt.Sprintf("tables=%d", countTables(restoredRowCounts)),
		"collationMismatches=0",
		fmt.Sprintf("proofDatabase=%s", postgresqlProofDatabase),
		fmt.Sprintf("proofValue=%s", postgresqlProofValue),
	)

	By("restoring the original Backstage replica configuration")
	restoreBackstageReplicas(namespace, crName, state.originalReplicas)
	waitForBackstagePodReplacement(namespace, crName, state.backstagePodUID)
}

func waitForPostgresql(namespace, crName, imageSubstring, versionPrefix string) string {
	var podName string
	label := fmt.Sprintf("rhdh.redhat.com/app=%s", postgresqlStatefulSetName(crName))

	Eventually(func(g Gomega) {
		out, err := helper.Run(exec.Command(helper.GetPlatformTool(), "-n", namespace, "get", "pods", "-l", label,
			"-o", "go-template={{ range .items }}{{ if not .metadata.deletionTimestamp }}{{ .metadata.name }}{{ \"\\n\" }}{{ end }}{{ end }}"))
		g.Expect(err).NotTo(HaveOccurred())
		podNames := helper.GetNonEmptyLines(string(out))
		g.Expect(podNames).To(HaveLen(1))
		podName = podNames[0]

		out, err = helper.Run(exec.Command(helper.GetPlatformTool(), "-n", namespace, "get", "pod", podName,
			"-o", `jsonpath={.status.conditions[?(@.type=="Ready")].status}`))
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(strings.TrimSpace(string(out))).To(Equal("True"))

		out, err = helper.Run(exec.Command(helper.GetPlatformTool(), "-n", namespace, "get", "pod", podName,
			"-o", "jsonpath={.spec.containers[0].image}"))
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(strings.TrimSpace(string(out))).To(ContainSubstring(imageSubstring))

		out, err = helper.Run(postgresqlSQLCommand(namespace, podName, "postgres", "-tA", "-c", "SHOW server_version;"))
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(strings.TrimSpace(string(out))).To(HavePrefix(versionPrefix))
	}, 10*time.Minute, 10*time.Second).Should(Succeed())

	return podName
}

func postgresqlSQLCommand(namespace, podName, database string, args ...string) *exec.Cmd {
	return buildPostgresqlSQLCommand(namespace, podName, database, false, args...)
}

func postgresqlSQLCommandWithStdin(namespace, podName, database string, args ...string) *exec.Cmd {
	return buildPostgresqlSQLCommand(namespace, podName, database, true, args...)
}

func buildPostgresqlSQLCommand(namespace, podName, database string, withStdin bool, args ...string) *exec.Cmd {
	commandArgs := []string{"-n", namespace, "exec"}
	if withStdin {
		commandArgs = append(commandArgs, "-i")
	}
	commandArgs = append(commandArgs,
		podName, "--",
		"psql", "-X", "-U", "postgres", "-d", database,
	)
	commandArgs = append(commandArgs, args...)
	return exec.Command(helper.GetPlatformTool(), commandArgs...)
}

func runPostgresqlSQL(namespace, podName, database, sql string) ([]byte, error) {
	return helper.Run(postgresqlSQLCommand(namespace, podName, database,
		"-v", "ON_ERROR_STOP=1", "-c", sql))
}

func queryPostgresqlUpgradeProof(namespace, podName string) string {
	out, err := helper.Run(postgresqlSQLCommand(namespace, podName, postgresqlProofDatabase,
		"-tA", "-v", "ON_ERROR_STOP=1", "-c", "SELECT marker FROM public.rhdh_postgresql_upgrade_test;"))
	Expect(err).NotTo(HaveOccurred())
	return strings.TrimSpace(string(out))
}

func dumpPostgresql(namespace, podName string) string {
	dumpFile, err := os.CreateTemp("", "rhdh-postgresql-15-*.sql")
	Expect(err).NotTo(HaveOccurred())
	dumpPath := dumpFile.Name()
	DeferCleanup(func() {
		Expect(os.Remove(dumpPath)).To(Succeed())
	})

	var stderr bytes.Buffer
	cmd := exec.Command(helper.GetPlatformTool(), "-n", namespace, "exec", podName, "--", "pg_dumpall", "-U", "postgres")
	err = runCommand(cmd, nil, dumpFile, io.MultiWriter(GinkgoWriter, &stderr))
	closeErr := dumpFile.Close()
	Expect(err).NotTo(HaveOccurred(), stderr.String())
	Expect(closeErr).NotTo(HaveOccurred())

	info, err := os.Stat(dumpPath)
	Expect(err).NotTo(HaveOccurred())
	Expect(info.Size()).To(BeNumerically(">", 0))
	logPostgresqlUpgradeCheckpoint("dump-created", fmt.Sprintf("bytes=%d", info.Size()))
	return dumpPath
}

func restorePostgresql(namespace, podName, dumpPath string) {
	dumpFile, err := os.Open(dumpPath)
	Expect(err).NotTo(HaveOccurred())
	defer func() {
		Expect(dumpFile.Close()).To(Succeed())
	}()

	var stdout, stderr bytes.Buffer
	cmd := postgresqlSQLCommandWithStdin(namespace, podName, "postgres", "-v", "ON_ERROR_STOP=0", "--quiet")
	err = runCommand(cmd, dumpFile, io.MultiWriter(GinkgoWriter, &stdout), io.MultiWriter(GinkgoWriter, &stderr))
	Expect(err).NotTo(HaveOccurred(), stderr.String())

	Expect(validatePostgresqlRestoreErrors(stderr.String())).To(Succeed())
}

func validatePostgresqlRestoreErrors(stderr string) error {
	errorLines := make([]string, 0)
	for _, line := range strings.Split(stderr, "\n") {
		lowerLine := strings.ToLower(line)
		if strings.Contains(line, "ERROR:") || strings.Contains(line, "FATAL:") || strings.Contains(lowerLine, "psql: error:") {
			errorLines = append(errorLines, strings.TrimSpace(line))
		}
	}
	unexpected := make([]string, 0)
	roleConflict := false
	for _, line := range errorLines {
		if !postgresqlRestoreBootstrapConflict.MatchString(line) {
			unexpected = append(unexpected, line)
			continue
		}
		if strings.Contains(line, "role") {
			roleConflict = true
		}
	}
	if len(unexpected) > 0 {
		return fmt.Errorf("unexpected PostgreSQL restore errors: %s", strings.Join(unexpected, "; "))
	}
	// pg_dumpall always recreates the bootstrap superuser; seeing the conflict proves the role section was replayed.
	if !roleConflict {
		return fmt.Errorf("expected the bootstrap role conflict, got: %q", errorLines)
	}
	return nil
}

func postgresqlDatabaseNames(namespace, podName string) []string {
	out, err := helper.Run(postgresqlSQLCommand(namespace, podName, "postgres",
		"-tA", "-v", "ON_ERROR_STOP=1", "-c",
		"SELECT datname FROM pg_database WHERE datistemplate = false ORDER BY datname;"))
	Expect(err).NotTo(HaveOccurred())
	return helper.GetNonEmptyLines(strings.TrimSpace(string(out)))
}

// postgresqlTableRowCountsSQL lists every user table with its exact row count, one "schema.table=count" per line.
// The same query is documented in docs/postgresql-upgrade.md.
const postgresqlTableRowCountsSQL = `SELECT format('%I.%I=%s', table_schema, table_name,
  (xpath('/row/c/text()', query_to_xml(format('SELECT count(*) AS c FROM %I.%I', table_schema, table_name), false, true, '')))[1]::text)
FROM information_schema.tables
WHERE table_type = 'BASE TABLE' AND table_schema NOT IN ('pg_catalog', 'information_schema')
ORDER BY table_schema, table_name;`

func postgresqlTableRowCounts(namespace, podName string, databases []string) map[string][]string {
	counts := make(map[string][]string, len(databases))
	for _, database := range databases {
		out, err := helper.Run(postgresqlSQLCommand(namespace, podName, database,
			"-tA", "-v", "ON_ERROR_STOP=1", "-c", postgresqlTableRowCountsSQL))
		Expect(err).NotTo(HaveOccurred())
		counts[database] = helper.GetNonEmptyLines(strings.TrimSpace(string(out)))
	}
	return counts
}

// hasBackstageOwnedRows reports whether a database contains rows in at least one table that was not created by this test.
func hasBackstageOwnedRows(tableRowCounts []string) bool {
	for _, line := range tableRowCounts {
		table, count, found := strings.Cut(line, "=")
		if found && table != "public.rhdh_postgresql_upgrade_test" && count != "0" {
			return true
		}
	}
	return false
}

func countTables(tableRowCounts map[string][]string) int {
	total := 0
	for _, tables := range tableRowCounts {
		total += len(tables)
	}
	return total
}

// postgresqlCollationMismatches returns the databases whose recorded collation version differs from the version
// provided by the operating system. A logical restore recreates every database, so none are expected.
func postgresqlCollationMismatches(namespace, podName string) []string {
	out, err := helper.Run(postgresqlSQLCommand(namespace, podName, "postgres",
		"-tA", "-v", "ON_ERROR_STOP=1", "-c",
		"SELECT datname FROM pg_database WHERE datallowconn AND datcollversion IS DISTINCT FROM pg_database_collation_actual_version(oid) ORDER BY datname;"))
	Expect(err).NotTo(HaveOccurred())
	return helper.GetNonEmptyLines(strings.TrimSpace(string(out)))
}

func runCommand(cmd *exec.Cmd, stdin io.Reader, stdout, stderr io.Writer) error {
	projectDir, err := helper.GetProjectDir()
	if err != nil {
		return err
	}
	cmd.Dir = projectDir
	cmd.Env = os.Environ()
	cmd.Stdin = stdin
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	GinkgoWriter.Printf("running: %s\n", strings.Join(cmd.Args, " "))
	return cmd.Run()
}

func setBackstageReplicas(namespace, crName string, replicas int) {
	patch := fmt.Sprintf(`{"spec":{"deployment":{"patch":{"spec":{"replicas":%d}}}}}`, replicas)
	err := helper.PatchBackstageCR(namespace, crName, patch, "merge")
	Expect(err).NotTo(HaveOccurred())
}

func backstagePatchReplicas(namespace, crName string) string {
	out, err := helper.Run(exec.Command(helper.GetPlatformTool(), "-n", namespace, "get", "backstage", crName,
		"-o", "jsonpath={.spec.deployment.patch.spec.replicas}"))
	Expect(err).NotTo(HaveOccurred())
	return strings.TrimSpace(string(out))
}

// restoreBackstageReplicas puts back the replica setting recorded before the upgrade. When none was set, the field is
// removed instead of forcing a value, so the default (or an HPA) controls the replica count again.
func restoreBackstageReplicas(namespace, crName, originalReplicas string) {
	value := "null"
	if originalReplicas != "" {
		value = originalReplicas
	}
	patch := fmt.Sprintf(`{"spec":{"deployment":{"patch":{"spec":{"replicas":%s}}}}}`, value)
	err := helper.PatchBackstageCR(namespace, crName, patch, "merge")
	Expect(err).NotTo(HaveOccurred())
	Expect(backstagePatchReplicas(namespace, crName)).To(Equal(originalReplicas))
}

func setIdleAnnotation(namespace, crName string, idle bool) {
	annotation := "rhdh.redhat.com/idle=true"
	if !idle {
		annotation = "rhdh.redhat.com/idle-"
	}
	_, err := helper.Run(exec.Command(helper.GetPlatformTool(), "-n", namespace, "annotate", "backstage", crName,
		annotation, "--overwrite"))
	Expect(err).NotTo(HaveOccurred())
}

func waitForNoPods(namespace, label string) {
	Eventually(func(g Gomega) {
		out, err := helper.Run(exec.Command(helper.GetPlatformTool(), "-n", namespace, "get", "pods", "-l", label,
			"-o", "name"))
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(strings.TrimSpace(string(out))).To(BeEmpty())
	}, 5*time.Minute, 5*time.Second).Should(Succeed())
}

func deleteResource(namespace, kind, name string) {
	_, err := helper.Run(exec.Command(helper.GetPlatformTool(), "-n", namespace, "delete", kind, name,
		"--wait=true", "--timeout=5m"))
	Expect(err).NotTo(HaveOccurred())
}

func resourceUID(namespace, kind, name string) string {
	out, err := helper.Run(exec.Command(helper.GetPlatformTool(), "-n", namespace, "get", kind, name,
		"-o", "jsonpath={.metadata.uid}"))
	Expect(err).NotTo(HaveOccurred())
	uid := strings.TrimSpace(string(out))
	Expect(uid).NotTo(BeEmpty())
	return uid
}

func readyPodUID(namespace, label string) string {
	var uid string
	Eventually(func(g Gomega) {
		out, err := helper.Run(exec.Command(helper.GetPlatformTool(), "-n", namespace, "get", "pods", "-l", label,
			"-o", `jsonpath={range .items[?(@.status.phase=="Running")]}{.metadata.uid}{" "}{.status.conditions[?(@.type=="Ready")].status}{"\n"}{end}`))
		g.Expect(err).NotTo(HaveOccurred())
		lines := helper.GetNonEmptyLines(string(out))
		g.Expect(lines).To(HaveLen(1))
		fields := strings.Fields(lines[0])
		g.Expect(fields).To(HaveLen(2))
		g.Expect(fields[1]).To(Equal("True"))
		uid = fields[0]
	}, 10*time.Minute, 10*time.Second).Should(Succeed())
	return uid
}

func waitForBackstagePodReplacement(namespace, crName, previousUID string) {
	label := fmt.Sprintf("rhdh.redhat.com/app=backstage-%s", crName)
	var replacementUID string
	Eventually(func(g Gomega) {
		out, err := helper.Run(exec.Command(helper.GetPlatformTool(), "-n", namespace, "get", "pods", "-l", label,
			"-o", `jsonpath={range .items[*]}{.metadata.uid}{" "}{.status.phase}{" "}{.status.conditions[?(@.type=="Ready")].status}{"\n"}{end}`))
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(string(out)).NotTo(ContainSubstring(previousUID))
		lines := helper.GetNonEmptyLines(string(out))
		g.Expect(lines).To(HaveLen(1))
		fields := strings.Fields(lines[0])
		g.Expect(fields).To(HaveLen(3))
		g.Expect(fields[0]).NotTo(Equal(previousUID))
		g.Expect(fields[1]).To(Equal("Running"))
		g.Expect(fields[2]).To(Equal("True"))
		replacementUID = fields[0]
	}, 15*time.Minute, 10*time.Second).Should(Succeed())
	logPostgresqlUpgradeCheckpoint("backstage-ready",
		fmt.Sprintf("previousPodUID=%s", previousUID),
		fmt.Sprintf("podUID=%s", replacementUID),
	)
}

func postgresqlStatefulSetName(crName string) string {
	return fmt.Sprintf("backstage-psql-%s", crName)
}

func postgresqlPVCName(crName string) string {
	return fmt.Sprintf("data-%s-0", postgresqlStatefulSetName(crName))
}

func postgresqlSecretName(crName string) string {
	return fmt.Sprintf("backstage-psql-secret-%s", crName)
}
