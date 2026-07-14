/*
# Enterprise Financial Transaction Clearing House

## What it is

A concurrent state engine utilizing atomic memory operations, thread-safe
collections (`ConcurrentHashMap`), and sliding-window statistics to safely
process multi-threaded transactions:

- **Immutable domain models** - `Transaction` and `Account` are Java records so
  identity and transfer payloads cannot be mutated after creation.
- **Lock-ordered settlement** - Per-account `ReentrantLock` pairs protect
  `AtomicLong` balances; locks are always acquired in sorted account-id order
  to prevent deadlock under high fan-out transfer traffic.
- **Sliding-window fraud guard** - High-volume attempts are tracked in a
  per-account time window; a fourth spike inside the window is intercepted
  before balances move.
- **Virtual-thread workers** - An `ExecutorService` backed by virtual threads
  drives rapid parallel transfers while a `SwingWorker` keeps the EDT free.

## What it is used for

High-throughput core banking systems, digital payment clearing gateways, and
real-time automated fraud detection pipelines:

- **Core banking settlement** - Demonstrate race-free debit/credit clearing
  across many accounts under parallel workers.
- **Payment clearing gateways** - Parameterized throughput, transfer caps, and
  volatility model bursty gateway traffic without code changes.
- **Real-time fraud pipelines** - Intercept accounts that attempt more than
  three high-volume transfers inside a short sliding timeframe.

Compile and run (JDK 21+):

    javac ClearingHouseSimulator.java
    java ClearingHouseSimulator --accounts 64 --threads 16 --max-transfer 50000 --volatility 0.35
*/

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Dimension;
import java.awt.Font;
import java.awt.Graphics;
import java.awt.Graphics2D;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.RenderingHints;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReentrantLock;

import javax.swing.BorderFactory;
import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JSpinner;
import javax.swing.SpinnerNumberModel;
import javax.swing.SwingUtilities;
import javax.swing.SwingWorker;
import javax.swing.Timer;
import javax.swing.UIManager;
import javax.swing.WindowConstants;
import javax.swing.border.EmptyBorder;

/**
 * Self-contained Enterprise Financial Transaction Clearing House simulator
 * with concurrent settlement, sliding-window fraud detection, and a live Swing UI.
 */
public class ClearingHouseSimulator {

    // -------------------------------------------------------------------------
    // Immutable domain models
    // -------------------------------------------------------------------------

    /** Immutable payment instruction cleared (or blocked) by the engine. */
    public record Transaction(
            String id,
            String fromId,
            String toId,
            long amountCents,
            long timestampMillis
    ) {
        public Transaction {
            Objects.requireNonNull(id, "id");
            Objects.requireNonNull(fromId, "fromId");
            Objects.requireNonNull(toId, "toId");
            if (amountCents <= 0) {
                throw new IllegalArgumentException("amountCents must be positive");
            }
        }
    }

    /** Immutable account identity; mutable balances live in {@link AccountState}. */
    public record Account(String id) {
        public Account {
            Objects.requireNonNull(id, "id");
            if (id.isBlank()) {
                throw new IllegalArgumentException("id must not be blank");
            }
        }
    }

    /**
     * Fully parameterized simulation knobs. Defaults live only here so runtime
     * logic never embeds magic limits.
     */
    public record SimulationConfig(
            int accountCount,
            int workerThreads,
            long maxTransferCents,
            double volatility,
            long fraudWindowMillis,
            long highVolumeThresholdCents,
            long initialBalanceCents
    ) {
        public static final SimulationConfig DEFAULTS = new SimulationConfig(
                64,
                16,
                50_000L,
                0.35,
                2_000L,
                0L,
                1_000_000L
        );

        public SimulationConfig {
            if (accountCount < 2) {
                throw new IllegalArgumentException("accountCount must be >= 2");
            }
            if (workerThreads < 1) {
                throw new IllegalArgumentException("workerThreads must be >= 1");
            }
            if (maxTransferCents < 1) {
                throw new IllegalArgumentException("maxTransferCents must be >= 1");
            }
            if (volatility < 0.0 || volatility > 1.0) {
                throw new IllegalArgumentException("volatility must be in [0, 1]");
            }
            if (fraudWindowMillis < 100) {
                throw new IllegalArgumentException("fraudWindowMillis must be >= 100");
            }
            if (initialBalanceCents < 0) {
                throw new IllegalArgumentException("initialBalanceCents must be >= 0");
            }
        }

        /** Derive a concrete high-volume threshold when the caller passes 0. */
        public long resolvedHighVolumeThreshold() {
            if (highVolumeThresholdCents > 0) {
                return highVolumeThresholdCents;
            }
            long derived = Math.round(maxTransferCents * Math.max(volatility, 0.15));
            return Math.max(1L, derived);
        }

        public SimulationConfig withDerivedThreshold() {
            return new SimulationConfig(
                    accountCount,
                    workerThreads,
                    maxTransferCents,
                    volatility,
                    fraudWindowMillis,
                    resolvedHighVolumeThreshold(),
                    initialBalanceCents
            );
        }
    }

    // -------------------------------------------------------------------------
    // Concurrent account / fraud state
    // -------------------------------------------------------------------------

    static final class AccountState {
        final Account account;
        final AtomicLong balanceCents;
        final ReentrantLock lock = new ReentrantLock();
        private final Deque<Long> highVolumeTimestamps = new ArrayDeque<>();
        private final ReentrantLock fraudLock = new ReentrantLock();

        volatile long activeUntilMillis;
        volatile long fraudUntilMillis;

        AccountState(Account account, long initialBalanceCents) {
            this.account = account;
            this.balanceCents = new AtomicLong(initialBalanceCents);
        }

        String id() {
            return account.id();
        }

        void markActive(long durationMillis) {
            activeUntilMillis = System.currentTimeMillis() + durationMillis;
        }

        void markFraud(long durationMillis) {
            fraudUntilMillis = System.currentTimeMillis() + durationMillis;
        }

        boolean isActive(long now) {
            return now < activeUntilMillis;
        }

        boolean isFraudLocked(long now) {
            return now < fraudUntilMillis;
        }
    }

    /**
     * Sliding-window guard: more than 3 high-volume attempts inside
     * {@code fraudWindowMillis} causes an intercept.
     */
    static final class FraudGuard {
        private static final int MAX_HIGH_VOLUME_IN_WINDOW = 3;

        private final long windowMillis;
        private final long highVolumeThresholdCents;

        FraudGuard(SimulationConfig config) {
            this.windowMillis = config.fraudWindowMillis();
            this.highVolumeThresholdCents = config.resolvedHighVolumeThreshold();
        }

        boolean isHighVolume(long amountCents) {
            return amountCents >= highVolumeThresholdCents;
        }

        /**
         * @return {@code true} if the transfer should be blocked
         */
        boolean wouldBlock(AccountState state, long amountCents, long nowMillis) {
            if (!isHighVolume(amountCents)) {
                return false;
            }
            state.fraudLock.lock();
            try {
                prune(state.highVolumeTimestamps, nowMillis);
                return state.highVolumeTimestamps.size() >= MAX_HIGH_VOLUME_IN_WINDOW;
            } finally {
                state.fraudLock.unlock();
            }
        }

        void recordSuccessfulHighVolume(AccountState state, long amountCents, long nowMillis) {
            if (!isHighVolume(amountCents)) {
                return;
            }
            state.fraudLock.lock();
            try {
                prune(state.highVolumeTimestamps, nowMillis);
                state.highVolumeTimestamps.addLast(nowMillis);
            } finally {
                state.fraudLock.unlock();
            }
        }

        private void prune(Deque<Long> timestamps, long nowMillis) {
            long cutoff = nowMillis - windowMillis;
            while (!timestamps.isEmpty() && timestamps.peekFirst() < cutoff) {
                timestamps.removeFirst();
            }
        }
    }

    // -------------------------------------------------------------------------
    // UI event / metrics bridge
    // -------------------------------------------------------------------------

    enum UiEventType {
        METRICS,
        ACTIVE,
        FRAUD,
        RESET_GRID
    }

    record UiEvent(
            UiEventType type,
            String accountId,
            double tps,
            long cleared,
            long blocked,
            int accountCount
    ) {
        static UiEvent metrics(double tps, long cleared, long blocked) {
            return new UiEvent(UiEventType.METRICS, null, tps, cleared, blocked, 0);
        }

        static UiEvent active(String accountId) {
            return new UiEvent(UiEventType.ACTIVE, accountId, 0, 0, 0, 0);
        }

        static UiEvent fraud(String accountId) {
            return new UiEvent(UiEventType.FRAUD, accountId, 0, 0, 0, 0);
        }

        static UiEvent resetGrid(int accountCount) {
            return new UiEvent(UiEventType.RESET_GRID, null, 0, 0, 0, accountCount);
        }
    }

    // -------------------------------------------------------------------------
    // Clearing engine (runs off the EDT)
    // -------------------------------------------------------------------------

    static final class ClearingEngine {
        private final SimulationConfig config;
        private final ConcurrentHashMap<String, AccountState> ledger = new ConcurrentHashMap<>();
        private final FraudGuard fraudGuard;
        private final LongAdder cleared = new LongAdder();
        private final LongAdder blocked = new LongAdder();
        private final AtomicLong transactionSeq = new AtomicLong();
        private final AtomicBoolean running = new AtomicBoolean(false);

        private ExecutorService executor;
        private final List<Future<?>> workerFutures = new ArrayList<>();
        private volatile long tpsWindowStartMillis;
        private volatile long tpsWindowClearedBaseline;
        private volatile double currentTps;

        ClearingEngine(SimulationConfig config) {
            this.config = config.withDerivedThreshold();
            this.fraudGuard = new FraudGuard(this.config);
            seedAccounts();
        }

        SimulationConfig config() {
            return config;
        }

        ConcurrentHashMap<String, AccountState> ledger() {
            return ledger;
        }

        void seedAccounts() {
            ledger.clear();
            for (int i = 0; i < config.accountCount(); i++) {
                String id = String.format(Locale.ROOT, "ACC-%04d", i);
                ledger.put(id, new AccountState(new Account(id), config.initialBalanceCents()));
            }
        }

        void start(java.util.function.Consumer<UiEvent> eventSink) {
            if (!running.compareAndSet(false, true)) {
                return;
            }
            cleared.reset();
            blocked.reset();
            transactionSeq.set(0);
            tpsWindowStartMillis = System.currentTimeMillis();
            tpsWindowClearedBaseline = 0;
            currentTps = 0;
            eventSink.accept(UiEvent.resetGrid(config.accountCount()));

            executor = Executors.newVirtualThreadPerTaskExecutor();
            workerFutures.clear();
            for (int i = 0; i < config.workerThreads(); i++) {
                final int workerId = i;
                workerFutures.add(executor.submit(() -> workerLoop(workerId, eventSink)));
            }
            workerFutures.add(executor.submit(() -> metricsLoop(eventSink)));
        }

        void stop() {
            running.set(false);
            for (Future<?> future : workerFutures) {
                future.cancel(true);
            }
            workerFutures.clear();
            if (executor != null) {
                executor.shutdownNow();
                try {
                    executor.awaitTermination(2, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                executor = null;
            }
        }

        boolean isRunning() {
            return running.get();
        }

        private void workerLoop(int workerId, java.util.function.Consumer<UiEvent> eventSink) {
            ThreadLocalRandom random = ThreadLocalRandom.current();
            List<AccountState> accounts = new ArrayList<>(ledger.values());
            while (running.get() && !Thread.currentThread().isInterrupted()) {
                try {
                    processOneTransfer(workerId, accounts, random, eventSink);
                    // Brief yield so UI flash windows remain visible under burst load
                    Thread.sleep(1 + (int) (config.volatility() * 4));
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (RuntimeException ex) {
                    // Keep the pool resilient; a single bad transfer must not halt clearing.
                }
            }
        }

        private void processOneTransfer(
                int workerId,
                List<AccountState> accounts,
                ThreadLocalRandom random,
                java.util.function.Consumer<UiEvent> eventSink
        ) {
            if (accounts.size() < 2) {
                return;
            }
            int fromIdx = random.nextInt(accounts.size());
            int toIdx;
            do {
                toIdx = random.nextInt(accounts.size());
            } while (toIdx == fromIdx);

            AccountState from = accounts.get(fromIdx);
            AccountState to = accounts.get(toIdx);
            long amount = sampleAmount(random);
            long now = System.currentTimeMillis();
            String txId = "TX-" + workerId + "-" + transactionSeq.incrementAndGet();
            Transaction tx = new Transaction(txId, from.id(), to.id(), amount, now);

            if (fraudGuard.wouldBlock(from, amount, now)) {
                from.markFraud(700);
                blocked.increment();
                eventSink.accept(UiEvent.fraud(from.id()));
                return;
            }

            if (!settle(from, to, tx.amountCents())) {
                return;
            }
            fraudGuard.recordSuccessfulHighVolume(from, amount, now);
            from.markActive(180);
            to.markActive(180);
            cleared.increment();
            eventSink.accept(UiEvent.active(from.id()));
            eventSink.accept(UiEvent.active(to.id()));
        }

        /**
         * Volatility tilts the amount distribution toward larger (high-volume) transfers.
         * amount = 1 + floor(U^(1 - volatility) * maxTransfer)
         */
        private long sampleAmount(Random random) {
            double u = random.nextDouble();
            double exponent = Math.max(0.05, 1.0 - config.volatility());
            double skewed = Math.pow(u, exponent);
            long amount = 1 + (long) Math.floor(skewed * (config.maxTransferCents() - 1));
            return Math.min(amount, config.maxTransferCents());
        }

        /** @return {@code true} if balances were updated */
        private boolean settle(AccountState a, AccountState b, long amountCents) {
            AccountState first = a.id().compareTo(b.id()) < 0 ? a : b;
            AccountState second = first == a ? b : a;

            first.lock.lock();
            second.lock.lock();
            try {
                if (a.balanceCents.get() < amountCents) {
                    return false;
                }
                a.balanceCents.addAndGet(-amountCents);
                b.balanceCents.addAndGet(amountCents);
                return true;
            } finally {
                second.lock.unlock();
                first.lock.unlock();
            }
        }

        private void metricsLoop(java.util.function.Consumer<UiEvent> eventSink) {
            while (running.get() && !Thread.currentThread().isInterrupted()) {
                try {
                    Thread.sleep(250);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
                long now = System.currentTimeMillis();
                long clearedNow = cleared.sum();
                long elapsed = now - tpsWindowStartMillis;
                if (elapsed >= 1_000L) {
                    long delta = clearedNow - tpsWindowClearedBaseline;
                    currentTps = delta * 1000.0 / elapsed;
                    tpsWindowStartMillis = now;
                    tpsWindowClearedBaseline = clearedNow;
                }
                eventSink.accept(UiEvent.metrics(currentTps, clearedNow, blocked.sum()));
            }
        }
    }

    // -------------------------------------------------------------------------
    // SwingWorker bridge
    // -------------------------------------------------------------------------

    static final class SimulationWorker extends SwingWorker<Void, UiEvent> {
        private final ClearingEngine engine;
        private final AccountGridPanel gridPanel;
        private final JLabel tpsLabel;
        private final JLabel clearedLabel;
        private final JLabel blockedLabel;
        private final JLabel statusLabel;
        private final Runnable onFinished;

        SimulationWorker(
                ClearingEngine engine,
                AccountGridPanel gridPanel,
                JLabel tpsLabel,
                JLabel clearedLabel,
                JLabel blockedLabel,
                JLabel statusLabel,
                Runnable onFinished
        ) {
            this.engine = engine;
            this.gridPanel = gridPanel;
            this.tpsLabel = tpsLabel;
            this.clearedLabel = clearedLabel;
            this.blockedLabel = blockedLabel;
            this.statusLabel = statusLabel;
            this.onFinished = onFinished;
        }

        @Override
        protected Void doInBackground() {
            engine.start(this::publishSafe);
            while (!isCancelled() && engine.isRunning()) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
            engine.stop();
            return null;
        }

        private void publishSafe(UiEvent event) {
            if (!isCancelled()) {
                publish(event);
            }
        }

        @Override
        protected void process(List<UiEvent> chunks) {
            for (UiEvent event : chunks) {
                switch (event.type()) {
                    case METRICS -> {
                        tpsLabel.setText(String.format(Locale.ROOT, "%.1f", event.tps()));
                        clearedLabel.setText(Long.toString(event.cleared()));
                        blockedLabel.setText(Long.toString(event.blocked()));
                    }
                    case ACTIVE -> gridPanel.flashActive(event.accountId());
                    case FRAUD -> gridPanel.flashFraud(event.accountId());
                    case RESET_GRID -> gridPanel.reset(event.accountCount());
                }
            }
        }

        @Override
        protected void done() {
            statusLabel.setText("Stopped");
            if (onFinished != null) {
                onFinished.run();
            }
        }

        void requestStop() {
            cancel(true);
            engine.stop();
        }
    }

    // -------------------------------------------------------------------------
    // Custom account grid
    // -------------------------------------------------------------------------

    static final class AccountGridPanel extends JPanel {
        private static final Color IDLE = new Color(0x2A2F3A);
        private static final Color ACTIVE = new Color(0x2F6FED);
        private static final Color FRAUD = new Color(0xC62828);
        private static final Color LOCKED = new Color(0x6A1B1B);
        private static final Color GRID_LINE = new Color(0x1A1E26);
        private static final Color LABEL = new Color(0xB0B8C4);

        private int accountCount;
        private final ConcurrentHashMap<String, Long> activeUntil = new ConcurrentHashMap<>();
        private final ConcurrentHashMap<String, Long> fraudUntil = new ConcurrentHashMap<>();
        private final Timer repaintTimer;

        AccountGridPanel(int initialCount) {
            this.accountCount = Math.max(2, initialCount);
            setBackground(new Color(0x12151C));
            setPreferredSize(new Dimension(720, 480));
            setBorder(BorderFactory.createEmptyBorder(8, 8, 8, 8));
            repaintTimer = new Timer(50, e -> repaint());
            repaintTimer.start();
        }

        void reset(int count) {
            accountCount = Math.max(2, count);
            activeUntil.clear();
            fraudUntil.clear();
            repaint();
        }

        void flashActive(String accountId) {
            if (accountId == null) {
                return;
            }
            activeUntil.put(accountId, System.currentTimeMillis() + 180);
        }

        void flashFraud(String accountId) {
            if (accountId == null) {
                return;
            }
            fraudUntil.put(accountId, System.currentTimeMillis() + 700);
        }

        void disposeTimer() {
            repaintTimer.stop();
        }

        @Override
        protected void paintComponent(Graphics g) {
            super.paintComponent(g);
            Graphics2D g2 = (Graphics2D) g.create();
            g2.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);

            int cols = (int) Math.ceil(Math.sqrt(accountCount));
            int rows = (int) Math.ceil(accountCount / (double) cols);
            int pad = 8;
            int width = getWidth() - pad * 2;
            int height = getHeight() - pad * 2;
            int cellW = Math.max(1, width / cols);
            int cellH = Math.max(1, height / rows);
            long now = System.currentTimeMillis();

            for (int i = 0; i < accountCount; i++) {
                int row = i / cols;
                int col = i % cols;
                int x = pad + col * cellW;
                int y = pad + row * cellH;
                String id = String.format(Locale.ROOT, "ACC-%04d", i);

                Long fraudTs = fraudUntil.get(id);
                Long activeTs = activeUntil.get(id);
                boolean fraud = fraudTs != null && now < fraudTs;
                boolean active = activeTs != null && now < activeTs;

                if (fraud) {
                    g2.setColor(LOCKED);
                    g2.fillRect(x + 1, y + 1, cellW - 2, cellH - 2);
                    g2.setColor(FRAUD);
                    g2.fillRect(x + 3, y + 3, cellW - 6, cellH - 6);
                } else if (active) {
                    g2.setColor(ACTIVE);
                    g2.fillRect(x + 1, y + 1, cellW - 2, cellH - 2);
                } else {
                    g2.setColor(IDLE);
                    g2.fillRect(x + 1, y + 1, cellW - 2, cellH - 2);
                }

                g2.setColor(GRID_LINE);
                g2.drawRect(x, y, cellW - 1, cellH - 1);

                if (cellW >= 48 && cellH >= 24) {
                    g2.setColor(LABEL);
                    g2.setFont(getFont().deriveFont(Font.PLAIN, 10f));
                    g2.drawString(id, x + 4, y + 14);
                }
            }
            g2.dispose();
        }
    }

    // -------------------------------------------------------------------------
    // Main window
    // -------------------------------------------------------------------------

    static final class ClearingHouseFrame extends JFrame {
        private final JSpinner accountsSpinner;
        private final JSpinner threadsSpinner;
        private final JSpinner maxTransferSpinner;
        private final JSpinner volatilitySpinner;
        private final JSpinner fraudWindowSpinner;
        private final JSpinner initialBalanceSpinner;
        private final JLabel tpsLabel = metricValue("0.0");
        private final JLabel clearedLabel = metricValue("0");
        private final JLabel blockedLabel = metricValue("0");
        private final JLabel statusLabel = new JLabel("Ready");
        private final JButton startButton = new JButton("Start");
        private final JButton stopButton = new JButton("Stop");
        private final AccountGridPanel gridPanel;
        private final SimulationConfig initialConfig;

        private SimulationWorker worker;
        private ClearingEngine engine;

        ClearingHouseFrame(SimulationConfig config) {
            super("Enterprise Financial Transaction Clearing House");
            this.initialConfig = config.withDerivedThreshold();
            this.gridPanel = new AccountGridPanel(initialConfig.accountCount());

            accountsSpinner = new JSpinner(new SpinnerNumberModel(initialConfig.accountCount(), 2, 400, 1));
            threadsSpinner = new JSpinner(new SpinnerNumberModel(initialConfig.workerThreads(), 1, 256, 1));
            maxTransferSpinner = new JSpinner(new SpinnerNumberModel(
                    (int) Math.min(Integer.MAX_VALUE, initialConfig.maxTransferCents()),
                    1,
                    Integer.MAX_VALUE,
                    1000
            ));
            volatilitySpinner = new JSpinner(new SpinnerNumberModel(initialConfig.volatility(), 0.0, 1.0, 0.05));
            fraudWindowSpinner = new JSpinner(new SpinnerNumberModel(
                    (int) Math.min(Integer.MAX_VALUE, initialConfig.fraudWindowMillis()),
                    100,
                    60_000,
                    100
            ));
            initialBalanceSpinner = new JSpinner(new SpinnerNumberModel(
                    (int) Math.min(Integer.MAX_VALUE, initialConfig.initialBalanceCents()),
                    0,
                    Integer.MAX_VALUE,
                    10_000
            ));

            setDefaultCloseOperation(WindowConstants.DISPOSE_ON_CLOSE);
            setLayout(new BorderLayout(8, 8));
            ((JPanel) getContentPane()).setBorder(new EmptyBorder(10, 10, 10, 10));

            add(buildControls(), BorderLayout.NORTH);
            add(gridPanel, BorderLayout.CENTER);
            add(buildScoreboard(), BorderLayout.SOUTH);

            stopButton.setEnabled(false);
            startButton.addActionListener(e -> startSimulation());
            stopButton.addActionListener(e -> stopSimulation());

            addWindowListener(new WindowAdapter() {
                @Override
                public void windowClosing(WindowEvent e) {
                    stopSimulation();
                    gridPanel.disposeTimer();
                }
            });

            pack();
            setLocationRelativeTo(null);
            setMinimumSize(new Dimension(820, 640));
        }

        private JPanel buildControls() {
            JPanel panel = new JPanel(new GridBagLayout());
            panel.setBorder(BorderFactory.createTitledBorder("Simulation Parameters"));
            GridBagConstraints c = new GridBagConstraints();
            c.insets = new Insets(4, 6, 4, 6);
            c.fill = GridBagConstraints.HORIZONTAL;
            c.gridy = 0;

            int col = 0;
            col = addLabeledSpinner(panel, c, col, "Accounts", accountsSpinner);
            col = addLabeledSpinner(panel, c, col, "Threads", threadsSpinner);
            col = addLabeledSpinner(panel, c, col, "Max Transfer (¢)", maxTransferSpinner);
            col = addLabeledSpinner(panel, c, col, "Volatility", volatilitySpinner);

            c.gridy = 1;
            col = 0;
            col = addLabeledSpinner(panel, c, col, "Fraud Window (ms)", fraudWindowSpinner);
            col = addLabeledSpinner(panel, c, col, "Initial Balance (¢)", initialBalanceSpinner);

            c.gridx = col;
            c.weightx = 0;
            panel.add(startButton, c);
            c.gridx = col + 1;
            panel.add(stopButton, c);
            c.gridx = col + 2;
            c.weightx = 1;
            statusLabel.setFont(statusLabel.getFont().deriveFont(Font.BOLD));
            panel.add(statusLabel, c);
            return panel;
        }

        private int addLabeledSpinner(
                JPanel panel,
                GridBagConstraints c,
                int col,
                String label,
                JSpinner spinner
        ) {
            c.gridx = col;
            c.weightx = 0;
            panel.add(new JLabel(label), c);
            c.gridx = col + 1;
            c.weightx = 0.5;
            panel.add(spinner, c);
            return col + 2;
        }

        private JPanel buildScoreboard() {
            JPanel panel = new JPanel(new GridBagLayout());
            panel.setBorder(BorderFactory.createTitledBorder("Live Clearing Scoreboard"));
            GridBagConstraints c = new GridBagConstraints();
            c.insets = new Insets(6, 12, 6, 12);
            c.gridy = 0;

            c.gridx = 0;
            panel.add(metricCaption("Throughput (TPS)"), c);
            c.gridx = 1;
            panel.add(tpsLabel, c);
            c.gridx = 2;
            panel.add(metricCaption("Cleared"), c);
            c.gridx = 3;
            panel.add(clearedLabel, c);
            c.gridx = 4;
            panel.add(metricCaption("Fraud Blocked"), c);
            c.gridx = 5;
            panel.add(blockedLabel, c);
            return panel;
        }

        private static JLabel metricCaption(String text) {
            JLabel label = new JLabel(text);
            label.setForeground(new Color(0x555555));
            return label;
        }

        private static JLabel metricValue(String text) {
            JLabel label = new JLabel(text);
            label.setFont(label.getFont().deriveFont(Font.BOLD, 18f));
            return label;
        }

        private SimulationConfig readConfigFromUi() {
            long maxTransfer = ((Number) maxTransferSpinner.getValue()).longValue();
            double volatility = ((Number) volatilitySpinner.getValue()).doubleValue();
            long derivedThreshold = Math.max(1L, Math.round(maxTransfer * Math.max(volatility, 0.15)));
            return new SimulationConfig(
                    ((Number) accountsSpinner.getValue()).intValue(),
                    ((Number) threadsSpinner.getValue()).intValue(),
                    maxTransfer,
                    volatility,
                    ((Number) fraudWindowSpinner.getValue()).longValue(),
                    derivedThreshold,
                    ((Number) initialBalanceSpinner.getValue()).longValue()
            );
        }

        private void setControlsEnabled(boolean enabled) {
            accountsSpinner.setEnabled(enabled);
            threadsSpinner.setEnabled(enabled);
            maxTransferSpinner.setEnabled(enabled);
            volatilitySpinner.setEnabled(enabled);
            fraudWindowSpinner.setEnabled(enabled);
            initialBalanceSpinner.setEnabled(enabled);
            startButton.setEnabled(enabled);
            stopButton.setEnabled(!enabled);
        }

        private void startSimulation() {
            if (worker != null && !worker.isDone()) {
                return;
            }
            SimulationConfig config = readConfigFromUi();
            engine = new ClearingEngine(config);
            setControlsEnabled(false);
            statusLabel.setText("Running (" + config.workerThreads() + " virtual threads)");
            tpsLabel.setText("0.0");
            clearedLabel.setText("0");
            blockedLabel.setText("0");

            worker = new SimulationWorker(
                    engine,
                    gridPanel,
                    tpsLabel,
                    clearedLabel,
                    blockedLabel,
                    statusLabel,
                    () -> setControlsEnabled(true)
            );
            worker.execute();
        }

        private void stopSimulation() {
            if (worker != null) {
                worker.requestStop();
            }
            if (engine != null) {
                engine.stop();
            }
        }
    }

    // -------------------------------------------------------------------------
    // CLI parsing + main
    // -------------------------------------------------------------------------

    static SimulationConfig parseArgs(String[] args) {
        int accounts = SimulationConfig.DEFAULTS.accountCount();
        int threads = SimulationConfig.DEFAULTS.workerThreads();
        long maxTransfer = SimulationConfig.DEFAULTS.maxTransferCents();
        double volatility = SimulationConfig.DEFAULTS.volatility();
        long fraudWindow = SimulationConfig.DEFAULTS.fraudWindowMillis();
        long highVolume = SimulationConfig.DEFAULTS.highVolumeThresholdCents();
        long initialBalance = SimulationConfig.DEFAULTS.initialBalanceCents();

        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            switch (arg) {
                case "--accounts" -> accounts = Integer.parseInt(requireValue(args, ++i, arg));
                case "--threads" -> threads = Integer.parseInt(requireValue(args, ++i, arg));
                case "--max-transfer" -> maxTransfer = Long.parseLong(requireValue(args, ++i, arg));
                case "--volatility" -> volatility = Double.parseDouble(requireValue(args, ++i, arg));
                case "--fraud-window-ms" -> fraudWindow = Long.parseLong(requireValue(args, ++i, arg));
                case "--high-volume" -> highVolume = Long.parseLong(requireValue(args, ++i, arg));
                case "--initial-balance" -> initialBalance = Long.parseLong(requireValue(args, ++i, arg));
                case "--help", "-h" -> {
                    printUsage();
                    System.exit(0);
                }
                default -> throw new IllegalArgumentException("Unknown argument: " + arg);
            }
        }

        return new SimulationConfig(
                accounts,
                threads,
                maxTransfer,
                volatility,
                fraudWindow,
                highVolume,
                initialBalance
        ).withDerivedThreshold();
    }

    private static String requireValue(String[] args, int index, String flag) {
        if (index >= args.length) {
            throw new IllegalArgumentException("Missing value for " + flag);
        }
        return args[index];
    }

    private static void printUsage() {
        System.out.println("""
                Usage: java ClearingHouseSimulator [options]

                  --accounts N           Number of bank accounts (default 64)
                  --threads N            Parallel transfer worker threads (default 16)
                  --max-transfer CENTS   Max transfer amount in cents (default 50000)
                  --volatility F         Amount skew factor in [0,1] (default 0.35)
                  --fraud-window-ms MS   Sliding fraud window (default 2000)
                  --high-volume CENTS    High-volume threshold (default: max-transfer * volatility)
                  --initial-balance C    Starting balance per account in cents (default 1000000)
                  --help                 Show this help
                """);
    }

    public static void main(String[] args) {
        final SimulationConfig config;
        try {
            config = parseArgs(args);
        } catch (RuntimeException ex) {
            System.err.println("Configuration error: " + ex.getMessage());
            printUsage();
            System.exit(1);
            return;
        }

        SwingUtilities.invokeLater(() -> {
            try {
                UIManager.setLookAndFeel(UIManager.getSystemLookAndFeelClassName());
            } catch (Exception ignored) {
                // Fall back to the default cross-platform L&F.
            }
            ClearingHouseFrame frame = new ClearingHouseFrame(config);
            frame.setVisible(true);
        });
    }
}
