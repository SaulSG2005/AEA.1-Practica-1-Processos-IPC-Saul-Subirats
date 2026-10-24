import java.io.BufferedReader;
import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * PAC1 · MP0490 · Supervisor de processos i comunicació IPC (tot en un sol fitxer).
 *
 * Una mateixa classe s'executa en JVM diferents; el PRIMER ARGUMENT decideix el rol:
 *   (cap)     -> SUPERVISOR : llança els fills i fa les demostracions
 *   worker    -> WORKER     : compta noms rebuts per stdin i fa N passos de feina
 *   producer  -> PRODUCER   : escriu cinc noms per stdout
 *   consumer  -> CONSUMER   : compta les línies que rep per stdin
 *   echo      -> ECHO       : passa a majúscules el que rep per stdin
 *
 * Compilar : javac --release 21 -d build echo.java
 * Executar : java -cp build echo
 *
 * REGLA D'OR: les DADES van per stdout; els DIAGNÒSTICS van per stderr.
 * Cada diagnòstic té el format [ROL pid=.. ppid=.. thread=..] missatge (escenari mínim).
 */
public class echo {

    // =====================================================================
    // 1. TIPUS DE DADES
    // =====================================================================

    /** Com ha acabat un procés. */
    enum Status { OK, FAILED, TIMEOUT_GRACEFUL, TIMEOUT_FORCED, NOT_STARTED }

    /** Una línia capturada: quan, d'on ("stdout"/"stderr") i el text. */
    record LogLine(Instant time, String source, String text) {}

    /** Resultat IMMUTABLE d'un procés (un record no es pot modificar un cop creat). */
    record ProcessResult(String role, List<String> command, long pid, long ppid, int exitCode,
                         Status status, Duration duration,
                         List<LogLine> stdout, List<LogLine> stderr) {
        ProcessResult {   // constructor compacte: còpies immutables de les llistes
            command = List.copyOf(command);
            stdout = List.copyOf(stdout);
            stderr = List.copyOf(stderr);
        }
        List<String> stdoutText() { return stdout.stream().map(LogLine::text).toList(); }
        List<String> stderrText() { return stderr.stream().map(LogLine::text).toList(); }
    }

    /** Recompte de línies i caràcters: el resultat de worker i consumer. */
    record Count(int lines, long chars) {
        static final Pattern FORMAT = Pattern.compile("COUNT=(\\d+) CHARS=(\\d+)");

        Count plus(Count other) { return new Count(lines + other.lines, chars + other.chars); }

        String format() { return "COUNT=" + lines + " CHARS=" + chars; }

        static Optional<Count> parse(String text) {
            Matcher m = FORMAT.matcher(text);
            return m.find()
                    ? Optional.of(new Count(Integer.parseInt(m.group(1)), Long.parseLong(m.group(2))))
                    : Optional.empty();
        }

        /** Llegeix fins a EOF. Decisió: les línies buides s'ignoren. */
        static Count read(BufferedReader reader) throws IOException {
            int lines = 0;
            long chars = 0;
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.isBlank()) continue;
                lines++;
                chars += line.strip().length();
            }
            return new Count(lines, chars);
        }
    }

    /** Qui escriu a stdin del fill. Es crida en un fil propi ("io-stdin"). */
    interface Feeder {
        void feed(OutputStream stdin) throws IOException, InterruptedException;
    }

    // =====================================================================
    // 2. CONSTANTS I ESTAT COMPARTIT
    // =====================================================================

    private static final Duration TIMEOUT = Duration.ofSeconds(15);
    private static final String SUPERVISOR = "SUPERVISOR";
    private static final DateTimeFormatter HORA =
            DateTimeFormatter.ofPattern("HH:mm:ss.SSS").withZone(ZoneId.systemDefault());

    // Processos fills vius ara mateix: el hook de tancament els mata si ens interrompen.
    private static final Set<Process> ACTIVE = ConcurrentHashMap.newKeySet();
    private static final AtomicInteger FAILURES = new AtomicInteger();

    // =====================================================================
    // 3. main: TRIAR EL ROL
    // =====================================================================

    public static void main(String[] args) throws Exception {
        // Forcem UTF-8 a la sortida perquè els accents surtin igual a tot arreu.
        System.setOut(new PrintStream(new FileOutputStream(FileDescriptor.out), true, StandardCharsets.UTF_8));
        System.setErr(new PrintStream(new FileOutputStream(FileDescriptor.err), true, StandardCharsets.UTF_8));

        if (args.length == 0) {
            runSupervisor();
        } else {
            switch (args[0]) {
                case "worker"   -> runWorker(args);
                case "producer" -> runProducer();
                case "consumer" -> runConsumer();
                case "echo"     -> runEchoWorker();
                default -> throw new IllegalArgumentException("Rol desconegut: " + args[0]);
            }
        }
    }

    // =====================================================================
    // 4. ROL SUPERVISOR
    // =====================================================================

    private static void runSupervisor() throws Exception {
        // Si algú interromp el supervisor (Ctrl+C, systemctl stop), matem els fills vius.
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            for (Process p : ACTIVE) killTree(p);
        }, "shutdown-hook"));

        log(System.out, SUPERVISOR, "començo");
        stdinDemo();
        parallelDemo();
        timeoutDemo();
        errorDemo();
        pipeDemo();
        log(System.out, SUPERVISOR, "acabo; comprovacions fallides = " + FAILURES.get());
        if (FAILURES.get() > 0) System.exit(1);   // perquè systemd vegi que ha fallat
    }

    /** Envia text a stdin d'un fill, tanca el flux i comprova que dades i diagnòstic van separats. */
    private static void stdinDemo() throws Exception {
        title("stdinDemo: echo (stdin -> majúscules -> stdout)");
        ProcessResult r = supervise("ECHO", command("echo"),
                text("Ebre\nprocessos\nFI\n"), null, TIMEOUT);
        show(r);
        check(r.status() == Status.OK, "el fill acaba bé");
        check(r.stdoutText().equals(List.of("EBRE", "PROCESSOS")), "stdout només porta les dades");
        check(r.stderr().stream().anyMatch(l -> l.text().contains("[ECHO")),
                "el diagnòstic va per stderr");
    }

    /** FORK: 3 workers independents amb un màxim de 2 alhora. JOIN: agreguem els resultats. */
    private static void parallelDemo() throws Exception {
        title("parallelDemo: 3 workers, màxim 2 alhora");
        List<String> names = List.of("ALPHA", "BETA", "GAMMA");
        List<List<String>> jobs = List.of(
                List.of("Ada", "Pol"), List.of("Joana", "Nil"), List.of("Marc"));
        AtomicInteger active = new AtomicInteger();
        AtomicInteger maxActive = new AtomicInteger();
        Instant t0 = Instant.now();
        List<ProcessResult> results = new ArrayList<>();

        // Un pool de DOS fils supervisors: cada fil llança un worker i hi espera fins que
        // acaba. Per això el límit cobreix tota la vida del worker, no només l'arrencada.
        try (ExecutorService pool = Executors.newFixedThreadPool(2)) {
            List<Future<ProcessResult>> futures = new ArrayList<>();
            for (int i = 0; i < jobs.size(); i++) {
                String name = names.get(i);
                String input = String.join("\n", jobs.get(i)) + "\n";
                futures.add(pool.submit(() -> {                                  // FORK
                    maxActive.accumulateAndGet(active.incrementAndGet(), Math::max);
                    try {
                        return supervise("WORKER-" + name, command("worker", name, "3", "200", "0"),
                                text(input), null, TIMEOUT);
                    } finally {
                        active.decrementAndGet();
                    }
                }));
            }
            for (Future<ProcessResult> f : futures) results.add(f.get());        // JOIN
        }
        long concurrentMs = Duration.between(t0, Instant.now()).toMillis();

        Count total = new Count(0, 0);
        long sequentialMs = 0;
        for (ProcessResult r : results) {
            show(r);
            sequentialMs += r.duration().toMillis();
            total = total.plus(r.stdoutText().stream().map(Count::parse)
                    .flatMap(Optional::stream).findFirst().orElse(new Count(0, 0)));
        }
        System.out.println("  TOTAL agregat: " + total.format());
        System.out.println("  durada concurrent = " + concurrentMs + " ms; suma seqüencial = " + sequentialMs + " ms");
        check(total.equals(new Count(5, 18)), "agregació correcta: COUNT=5 CHARS=18");
        check(maxActive.get() == 2, "mai més de 2 workers alhora (màxim observat = " + maxActive.get() + ")");
        check(concurrentMs < sequentialMs, "hi ha solapament real (concurrent < seqüencial)");
    }

    /** Un worker que triga 30 s amb un límit d'1 s: l'hem de matar i comprovar que no queda viu. */
    private static void timeoutDemo() throws Exception {
        title("timeoutDemo: worker de 100 passos d'1 s amb límit de 2,5 s");
        ProcessResult r = supervise("WORKER-LENT", command("worker", "LENT", "100", "1000", "0"),
                null, null, Duration.ofMillis(2500));
        show(r);
        boolean alive = ProcessHandle.of(r.pid()).map(ProcessHandle::isAlive).orElse(false);
        check(r.status() == Status.TIMEOUT_GRACEFUL || r.status() == Status.TIMEOUT_FORCED,
                "causa registrada: " + r.status());
        check(!alive, "el procés fill ja no és viu");
    }

    /** Distingir "falla" (codi 7) de "no arrenca" (ordre inexistent). */
    private static void errorDemo() throws Exception {
        title("errorDemo: codi de sortida 7 i ordre inexistent");
        ProcessResult r1 = supervise("WORKER-BETA", command("worker", "BETA", "3", "100", "7"),
                text("Ada\n"), null, TIMEOUT);
        show(r1);
        check(r1.status() == Status.FAILED && r1.exitCode() == 7, "FAILED amb codi 7 explícit");
        check(r1.stderrText().stream().anyMatch(s -> s.contains("codi 7")), "l'error queda a stderr");

        ProcessResult r2 = supervise("INEXISTENT", List.of("aquesta-ordre-no-existeix"),
                null, null, TIMEOUT);
        show(r2);
        check(r2.status() == Status.NOT_STARTED, "ordre inexistent -> NOT_STARTED");
    }

    /**
     * PRODUCER --stdout--> SUPERVISOR --stdin--> CONSUMER
     * L'únic lector del stdout del producer és el seu fil "io-stdout". Cada línia llegida es
     * posa en una cua; el fil "io-stdin" del consumer la treu de la cua i l'escriu al seu stdin.
     * Quan el producer acaba, posem una marca de final i el consumer veu EOF.
     */
    private static void pipeDemo() throws Exception {
        title("pipeDemo: producer -> supervisor -> consumer");
        BlockingQueue<Optional<String>> queue = new LinkedBlockingQueue<>();   // buit = EOF
        ProcessResult producer;
        ProcessResult consumer;
        try (ExecutorService pool = Executors.newFixedThreadPool(2)) {
            Future<ProcessResult> fProducer = pool.submit(() -> {
                try {
                    return supervise("PRODUCER", command("producer"), null,
                            line -> queue.add(Optional.of(line)), TIMEOUT);
                } finally {
                    queue.add(Optional.empty());   // passi el que passi, el consumer veurà EOF
                }
            });
            Future<ProcessResult> fConsumer = pool.submit(() ->
                    supervise("CONSUMER", command("consumer"), fromQueue(queue), null, TIMEOUT));
            producer = fProducer.get();
            consumer = fConsumer.get();
        }
        show(producer);
        show(consumer);
        check(producer.status() == Status.OK && consumer.status() == Status.OK, "tots dos acaben bé");
        check(consumer.stdoutText().contains("COUNT=5 CHARS=18"), "el consumer retorna COUNT=5 CHARS=18");
    }

    // =====================================================================
    // 5. EL NUCLI: supervise, drain, startProcess
    // =====================================================================

    /**
     * Executa UN procés i en retorna el resultat. Sempre el deixa mort en acabar.
     *  feeder    : què s'escriu a stdin (null = tanca stdin de seguida)
     *  stdoutTap : si no és null, rep cada línia de stdout (la pipe l'usa)
     */
    private static ProcessResult supervise(String role, List<String> command, Feeder feeder,
                                           Consumer<String> stdoutTap, Duration timeout)
            throws InterruptedException {
        Instant start = Instant.now();
        Process p;
        try {
            p = startProcess(command);
        } catch (IOException e) {   // ordre inexistent o sense permisos
            LogLine error = new LogLine(Instant.now(), "stderr", e.getMessage());
            return new ProcessResult(role, command, -1, -1, -1, Status.NOT_STARTED,
                    Duration.between(start, Instant.now()), List.of(), List.of(error));
        }
        ACTIVE.add(p);
        long ppid = p.toHandle().parent().map(ProcessHandle::pid).orElse(-1L);
        log(System.out, SUPERVISOR, "he llançat " + role + " (fill pid=" + p.pid() + ")");

        List<LogLine> out = Collections.synchronizedList(new ArrayList<>());
        List<LogLine> err = Collections.synchronizedList(new ArrayList<>());
        Thread stdinThread = null;
        try {
            // 1) Primer els lectors: així el fill mai es bloqueja escrivint.
            Thread outThread = drain(p.getInputStream(), "stdout", "io-stdout", out, stdoutTap);
            Thread errThread = drain(p.getErrorStream(), "stderr", "io-stderr", err, null);

            // 2) Escrivim a stdin en un fil propi i el tanquem SEMPRE (això és l'EOF del fill).
            stdinThread = Thread.ofPlatform().name("io-stdin").start(() -> {
                try (OutputStream stdin = p.getOutputStream()) {
                    if (feeder != null) feeder.feed(stdin);
                } catch (IOException | InterruptedException e) {
                    // El fill ha tancat stdin, ha mort o ens han cancel·lat: no és un error nostre.
                }
            });

            // 3) Esperem amb límit de temps.
            Status status;
            if (p.waitFor(timeout.toMillis(), TimeUnit.MILLISECONDS)) {
                status = p.exitValue() == 0 ? Status.OK : Status.FAILED;
            } else {
                status = terminate(p);
            }

            // 4) Esperem que els lectors acabin de buidar els fluxos.
            outThread.join(2000);
            errThread.join(2000);
            return new ProcessResult(role, command, p.pid(), ppid, p.exitValue(), status,
                    Duration.between(start, Instant.now()), out, err);
        } finally {
            // Xarxa de seguretat: error, cancel·lació o timeout -> el procés NO pot quedar viu.
            killTree(p);
            if (stdinThread != null) stdinThread.interrupt();
            ACTIVE.remove(p);
        }
    }

    /** Fil lector: llegeix línia a línia i desa hora + origen + text. */
    private static Thread drain(InputStream in, String source, String threadName,
                                List<LogLine> sink, Consumer<String> tap) {
        return Thread.ofPlatform().name(threadName).start(() -> {
            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(in, StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    sink.add(new LogLine(Instant.now(), source, line));
                    if (tap != null) tap.accept(line);
                }
            } catch (IOException e) {
                // Flux tancat (per exemple, perquè hem matat el procés): és normal.
            }
        });
    }

    /** Crea el procés a partir d'una LLISTA (sense shell: res no s'interpreta). */
    private static Process startProcess(List<String> command) throws IOException {
        return new ProcessBuilder(command).start();
    }

    /** Finalització ordenada (SIGTERM) i, si cal, forçada (SIGKILL), amb els descendents. */
    private static Status terminate(Process p) throws InterruptedException {
        List<ProcessHandle> descendants = p.descendants().toList();   // abans de matar el pare
        p.destroy();
        Status status = Status.TIMEOUT_GRACEFUL;
        if (!p.waitFor(2, TimeUnit.SECONDS)) {
            p.destroyForcibly();
            p.waitFor();
            status = Status.TIMEOUT_FORCED;
        }
        descendants.forEach(ProcessHandle::destroyForcibly);
        return status;
    }

    /** Mata el procés i tots els seus descendents si encara són vius. */
    private static void killTree(Process p) {
        List<ProcessHandle> descendants = p.descendants().toList();
        if (p.isAlive()) p.destroyForcibly();
        descendants.forEach(ProcessHandle::destroyForcibly);
    }

    // ---------- Feeders ----------

    private static Feeder text(String content) {
        return stdin -> stdin.write(content.getBytes(StandardCharsets.UTF_8));
    }

    /** Escriu al stdin del fill les línies que van arribant a la cua, fins a la marca d'EOF. */
    private static Feeder fromQueue(BlockingQueue<Optional<String>> queue) {
        return stdin -> {
            Writer writer = new OutputStreamWriter(stdin, StandardCharsets.UTF_8);
            boolean broken = false;
            Optional<String> item;
            while ((item = queue.take()).isPresent()) {
                if (broken) continue;   // si el consumer ha mort, seguim buidant la cua
                try {
                    writer.write(item.get() + "\n");
                    writer.flush();
                } catch (IOException e) {
                    broken = true;
                }
            }
        };
    }

    // =====================================================================
    // 6. ROLS FILLS (cada un s'executa en una JVM pròpia)
    // =====================================================================

    /** worker <nom> <passos> <msPerPas> <codiSortida>: compta noms de stdin i respon COUNT=.. CHARS=.. */
    private static void runWorker(String[] args) throws Exception {
        String name = args[1];
        int steps = Integer.parseInt(args[2]);
        long stepMs = Long.parseLong(args[3]);
        int exitCode = Integer.parseInt(args[4]);
        String role = "WORKER-" + name;

        log(System.err, role, "inici");
        Count count;
        try (BufferedReader in = new BufferedReader(
                new InputStreamReader(System.in, StandardCharsets.UTF_8))) {
            count = Count.read(in);
        }
        log(System.err, role, "he rebut " + count.format());
        for (int step = 1; step <= steps; step++) {
            Thread.sleep(stepMs);                          // simula feina
            log(System.err, role, "pas " + step + "/" + steps);
        }
        System.out.println(count.format());                // DADES -> stdout
        if (exitCode != 0) log(System.err, role, "error simulat, surto amb codi " + exitCode);
        System.exit(exitCode);
    }

    /** Escriu cinc noms per stdout (dades) i el seu diagnòstic per stderr. */
    private static void runProducer() throws Exception {
        for (String name : List.of("Ada", "Pol", "Joana", "Nil", "Marc")) {
            log(System.err, "PRODUCER", "envio " + name);
            System.out.println(name);
            Thread.sleep(100);
        }
    }

    /** Llegeix stdin fins a EOF (el supervisor el tanca quan el producer acaba). */
    private static void runConsumer() throws Exception {
        log(System.err, "CONSUMER", "espero dades per stdin");
        try (BufferedReader in = new BufferedReader(
                new InputStreamReader(System.in, StandardCharsets.UTF_8))) {
            System.out.println(Count.read(in).format());
        }
    }

    /** Exemple de la PAC: passa a majúscules fins a "FI" o fins a EOF. */
    private static void runEchoWorker() throws IOException {
        log(System.err, "ECHO", "inici");
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(System.in, StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null && !line.equals("FI")) {
                System.out.println(line.toUpperCase(Locale.ROOT));
            }
        }
    }

    // =====================================================================
    // 7. UTILITATS (diagnosi, ordres i presentació)
    // =====================================================================

    /** Etiqueta de l'escenari mínim: [ROL pid=.. ppid=.. thread=..] */
    private static String label(String role) {
        ProcessHandle me = ProcessHandle.current();
        long ppid = me.parent().map(ProcessHandle::pid).orElse(-1L);
        return "[%s pid=%d ppid=%d thread=%s]"
                .formatted(role, me.pid(), ppid, Thread.currentThread().getName());
    }

    /** Missatge de diagnosi: [ROL pid=.. ppid=.. thread=..] text */
    private static void log(PrintStream stream, String role, String message) {
        stream.println(label(role) + " " + message);
    }

    /** Missatge del supervisor per stdout. */
    private static void say(String message) {
        log(System.out, SUPERVISOR, message);
    }

    /** java -cp <classpath actual> Supervisor <args...>: una JVM nova amb un rol concret. */
    private static List<String> command(String... args) {
        String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        List<String> command = new ArrayList<>(List.of(java, "-cp",
                System.getProperty("java.class.path"), echo.class.getName()));
        command.addAll(List.of(args));
        return command;
    }

    private static void title(String text) {
        System.out.println("\n=== " + text + " ===");
    }

    private static void check(boolean ok, String what) {
        say((ok ? "[OK] " : "[ERROR] ") + what);
        if (!ok) FAILURES.incrementAndGet();
    }

    private static String describe(List<String> command) {
        return command.size() > 4 && command.get(1).equals("-cp")
                ? "java ... " + String.join(" ", command.subList(3, command.size()))
                : String.join(" ", command);
    }

    /**
     * Mostra un resultat amb l'escenari mínim. Les línies de diagnòstic dels fills ja arriben
     * etiquetades pel fill ([WORKER-BETA pid=.. ppid=.. thread=main] pas 2/3) i es mostren tal
     * qual; les DADES (stdout) les etiqueta el supervisor, que és qui les mostra.
     */
    private static void show(ProcessResult r) {
        say(r.role() + " -> " + r.status() + ", codi=" + r.exitCode()
                + ", durada=" + r.duration().toMillis() + " ms"
                + " (fill pid=" + r.pid() + ", ppid=" + r.ppid() + ")");
        say(r.role() + " ordre: " + describe(r.command()));
        Stream.concat(r.stdout().stream(), r.stderr().stream())
                .sorted(Comparator.comparing(LogLine::time))
                .forEach(l -> {
                    String hora = HORA.format(l.time());
                    if (l.source().equals("stderr") && l.text().startsWith("[")) {
                        System.out.println("  " + hora + " " + l.text());
                    } else {
                        System.out.println("  " + hora + " " + label(SUPERVISOR) + " "
                                + l.source() + " de " + r.role() + ": " + l.text());
                    }
                });
    }
}