import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

/*
 * ПРАКТИЧЕСКОЕ ЗАДАНИЕ: МНОГОПОТОЧНЫЙ АНАЛИЗ ЛОГОВ ВЕБ-СЕРВЕРА
 *
 * Крупные части задания:
 * 1. Сгенерировать набор настоящих файлов с логами.
 * 2. Реализовать разбор строк и последовательный отчёт.
 * 3. Распараллелить обработку нескольких файлов через ExecutorService.
 * 4. Сделать общую статистику потокобезопасной.
 * 5. Ограничить число одновременно читаемых файлов через Semaphore.
 * 6. Добавить Atomic-счётчики, CAS и корректное ожидание завершения.
 * 7. Реализовать досрочную остановку анализа через volatile.
 * 8. Сформулировать и доказать инварианты корректности и отсутствия дедлоков.
 * 9. Реализовать обработку повреждённых файлов и частичный отчёт.
 *
 * ФАЙЛ МОЖНО ЗАПУСТИТЬ САМОСТОЯТЕЛЬНО:
 *   java LogAnalysisAssignment generate logs 20 1000
 *   java LogAnalysisAssignment check
 *
 * Первая команда создаёт каталог logs и файлы access-000.log, access-001.log и т.д.
 * Аргументы: каталог, количество файлов, количество строк в каждом файле.
 * Вторая команда выполняет встроенную проверку генератора и итератора строк.
 *
 * ФОРМАТ СТРОКИ:
 *   METHOD PATH STATUS RESPONSE_BYTES DURATION_MILLIS
 * Например:
 *   GET /api/users 200 820 4
 *
 * Поля разделены одним или несколькими пробелами.
 * Успешный запрос имеет статус от 200 до 399 включительно.
 * Ошибочный запрос имеет статус от 400 и выше.
 *
 * ГОТОВЫЕ СТРУКТУРЫ:
 *   LogEntry       — одна разобранная запись.
 *   LogFile        — путь к одному файлу.
 *   PathStatistics — неизменяемая статистика одного URL.
 *   Report         — итоговый отчёт.
 *   LogFileLines   — итератор строк файла.
 *   GenerateLogs   — генератор входных файлов.
 *
 * ВАЖНО ПРО LogFileLines:
 * Итератор потоконебезопасен. Один экземпляр Iterator<String> нельзя читать
 * из нескольких потоков одновременно. Каждый поток должен получить собственный
 * итератор для своего файла. Нельзя разделить один итератор между задачами.
 * Сам файл можно читать параллельно разными потоками только при условии, что
 * каждый поток создаёт отдельный итератор.
 *
 * ЧАСТИ ЗАДАНИЯ:
 *
 * 1. РАЗБОР СТРОКИ
 * Реализуйте преобразование одной строки в LogEntry. Проверьте количество
 * полей, преобразуйте числовые значения и выбрасывайте понятную ошибку для
 * повреждённой строки.
 *
 * 2. ПОСЛЕДОВАТЕЛЬНЫЙ ОТЧЁТ
 * Напишите обработку всех файлов одним потоком. Для каждого URL собирайте
 * количество запросов, количество успешных запросов и ошибок, объём ответов,
 * суммарное и максимальное время обработки. Этот вариант станет эталоном для
 * проверки многопоточной версии.
 *
 * 3. ПАРАЛЛЕЛЬНАЯ ОБРАБОТКА ФАЙЛОВ
 * Создайте фиксированный ExecutorService. Каждый файл обрабатывается отдельной
 * задачей. Рабочий поток создаёт собственный LogFileLines.Iterator, читает свой
 * файл и передаёт записи в общий аккумулятор. Главный поток должен дождаться
 * завершения всех файлов и только потом построить Report.
 *
 * 4. ПОТОКОБЕЗОПАСНАЯ СТАТИСТИКА
 * Реализуйте аккумулятор статистики. Сначала допустимо использовать один общий
 * synchronized-метод. Затем реализуйте вариант с отдельным synchronized-аккумулятором
 * для каждого URL и ConcurrentHashMap. Сравните эти варианты на данных, где почти
 * все запросы относятся к одному URL, и на данных с большим числом URL.
 *
 * 5. ОГРАНИЧЕНИЕ ЧТЕНИЯ ФАЙЛОВ
 * Добавьте Semaphore, ограничивающий число файлов, которые могут обрабатываться
 * одновременно. Рабочих потоков может быть больше разрешений семафора. Разрешение
 * нужно получить перед чтением файла и гарантированно вернуть после завершения.
 * Добавьте наблюдение за текущим и максимальным числом активных файлов.
 *
 * 6. ATOMIC-СЧЁТЧИКИ И CAS
 * Добавьте счётчики обработанных файлов, прочитанных строк, корректных записей,
 * повреждённых записей, ошибок чтения и активных файлов. Для максимального числа
 * активных файлов используйте AtomicInteger и compareAndSet. Обычные изменяемые
 * int/long для этих глобальных счётчиков не используйте.
 *
 * 7. ОЖИДАНИЕ ЗАВЕРШЕНИЯ
 * Организуйте ожидание всех файлов через CountDownLatch либо через набор Future.
 * Завершение задачи должно учитываться даже при исключении. ExecutorService нужно
 * корректно закрыть и дождаться его завершения.
 *
 * CountDownLatch — это одноразовый счётчик-сигнал. Его создают с начальным
 * числом, например равным количеству файлов. Главный поток вызывает await()
 * и блокируется, пока счётчик не станет равен нулю. Каждая завершившаяся задача
 * один раз вызывает countDown(), уменьшая счётчик. Когда последний счётчик
 * уменьшен до нуля, все ожидающие потоки продолжают работу. Увеличить latch
 * обратно или использовать его повторно нельзя, поэтому для нового запуска
 * создаётся новый объект. Вызов countDown() нужно помещать в finally, чтобы
 * исключение в задаче не оставило главный поток ждать бесконечно.
 *
 * 8. ДОСРОЧНАЯ ОСТАНОВКА
 * Добавьте volatile-флаг остановки. После остановки новые строки не начинают
 * обрабатываться, а уже начатая операция может завершиться. Опишите, какие данные
 * входят в частичный отчёт. Не оставляйте рабочие потоки и разрешения семафора
 * в подвешенном состоянии.
 *
 * 9. ПОВРЕЖДЁННЫЕ ФАЙЛЫ И ЧАСТИЧНЫЙ ОТЧЁТ
 * Сделайте так, чтобы ошибка одного файла не отменяла обработку остальных файлов.
 * В итоговом результате отдельно храните список или число файлов, которые не удалось
 * обработать. Решите, что происходит при повреждённой строке: прекращается обработка
 * всего файла или пропускается только эта строка. Выберите один вариант и явно
 * зафиксируйте его в документации. Для полного запуска корректные файлы должны
 * давать полный отчёт, а ошибка должна быть видна пользователю.
 *
 * ИНВАРИАНТЫ, КОТОРЫЕ НУЖНО ДОКАЗАТЬ:
 * - каждая строка полного запуска обработана ровно один раз;
 * - для каждого URL requests = successful + errors;
 * - maxDuration <= totalDuration при наличии запросов;
 * - 0 <= activeFiles <= semaphoreLimit;
 * - после завершения анализа activeFiles равно нулю;
 * - один повреждённый файл не мешает обработке остальных;
 * - в программе нет дедлока.
 *
 * В доказательстве отсутствия дедлока укажите порядок захвата мониторов и
 * семафоров. Отдельно объясните, может ли поток ждать acquire(), удерживая
 * synchronized-монитор, и почему разрешение семафора возвращается при исключении.
 * Объясните, почему volatile нужен для флага остановки.
 *
 * НЕБОЛЬШИЕ ЭКСПЕРИМЕНТЫ ПОСЛЕ РЕАЛИЗАЦИИ:
 * - сравнить 1 и 4 рабочих потока;
 * - сравнить лимит семафора 1 и 4;
 * - сравнить глобальную блокировку и блокировки по URL;
 * - добавить одну повреждённую строку или один повреждённый файл и проверить,
 *   что остальные данные всё равно попадают в отчёт.
 * Эксперименты не требуют дополнительной архитектуры или новых классов.
 */

public class Main {
    interface Mutex {
        void lock();
        void unlock();
    }

    static final class SpinMutex implements Mutex {
        private final AtomicReference<Thread> owner = new AtomicReference<>();

        public void lock() {
            Thread current = Thread.currentThread();
            if (owner.get() == current) {
                throw new IllegalStateException();
            }
            while (!owner.compareAndSet(null, current)) {
                Thread.onSpinWait();
            }
        }

        public void unlock() {
            Thread current = Thread.currentThread();
            if (!owner.compareAndSet(current, null)) {
                throw new IllegalMonitorStateException();
            }
        }
    }

    static final class ReentrantSpinMutex implements Mutex {
        private final AtomicReference<Thread> owner = new AtomicReference<>();
        private int depth;

        public void lock() {
            Thread current = Thread.currentThread();
            if (owner.get() == current) {
                if (depth + 1 <= 0) {
                    throw new IllegalStateException();
                }
                depth++;
                return;
            }
            while (!owner.compareAndSet(null, current)) {
                Thread.onSpinWait();
            }
            depth = 1;
        }

        public void unlock() {
            Thread current = Thread.currentThread();
            if (owner.get() != current) {
                throw new IllegalMonitorStateException();
            }
            depth--;
            if (depth == 0) {
                owner.set(null);
            }
        }
    }

    static final class OneWaiterMutex implements Mutex {
        private final AtomicReference<Thread> owner = new AtomicReference<>();
        private final AtomicReference<Thread> waiter = new AtomicReference<>();

        public void lock() {
            Thread current = Thread.currentThread();
            if (owner.get() == current) {
                throw new IllegalStateException();
            }
            if (owner.compareAndSet(null, current)) {
                return;
            }

            waiter.set(current);
            boolean interrupted = false;
            try {
                while (!owner.compareAndSet(null, current)) {
                    LockSupport.park(this);
                    if (Thread.interrupted()) {
                        interrupted = true;
                    }
                }
            } finally {
                waiter.compareAndSet(current, null);
                if (interrupted) {
                    current.interrupt();
                }
            }
        }

        public void unlock() {
            Thread current = Thread.currentThread();
            if (!owner.compareAndSet(current, null)) {
                throw new IllegalMonitorStateException();
            }
            Thread w = waiter.get();
            if (w != null) {
                LockSupport.unpark(w);
            }
        }
    }

    static final class QueuedMutex implements Mutex {
        private final AtomicReference<Thread> owner = new AtomicReference<>();
        private final ConcurrentLinkedQueue<Thread> waiters = new ConcurrentLinkedQueue<>();

        public void lock() {
            Thread current = Thread.currentThread();
            if (owner.get() == current) {
                throw new IllegalStateException();
            }

            waiters.add(current);
            boolean interrupted = false;
            try {
                while (true) {
                    if (waiters.peek() == current && owner.compareAndSet(null, current)) {
                        waiters.poll();
                        return;
                    }
                    LockSupport.park(this);
                    if (Thread.interrupted()) {
                        interrupted = true;
                    }
                }
            } finally {
                if (interrupted) {
                    current.interrupt();
                }
            }
        }

        public void unlock() {
            Thread current = Thread.currentThread();
            if (!owner.compareAndSet(current, null)) {
                throw new IllegalMonitorStateException();
            }
            Thread next = waiters.peek();
            if (next != null) {
                LockSupport.unpark(next);
            }
        }
    }

    private static UnsupportedOperationException todo(int step) {
        return new UnsupportedOperationException("Сначала выполните шаг " + step);
    }

    private static final long TIMEOUT_MS = 10_000;
    private static final class Counter { long value; }

    private static void require(boolean condition, String message) {
        if (!condition) throw new AssertionError(message);
    }

    private static Thread launch(String name, AtomicReference<Throwable> failure,
                                 Runnable action) {
        Thread t = new Thread(() -> {
            try { action.run(); }
            catch (Throwable e) { failure.compareAndSet(null, e); }
        }, name);
        t.setDaemon(true);
        t.start();
        return t;
    }

    private static void joinAll(List<Thread> threads, AtomicReference<Throwable> failure)
            throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(TIMEOUT_MS);
        for (Thread t : threads) {
            while (t.isAlive() && failure.get() == null) {
                long left = deadline - System.nanoTime();
                if (left <= 0) throw new AssertionError("Таймаут: проверьте протокол ожидания");
                t.join(Math.min(100, Math.max(1, TimeUnit.NANOSECONDS.toMillis(left))));
            }
            if (failure.get() != null) throw new AssertionError("Ошибка в рабочем потоке", failure.get());
        }
    }

    private static void await(CountDownLatch latch) {
        try {
            if (!latch.await(TIMEOUT_MS, TimeUnit.MILLISECONDS))
                throw new AssertionError("Таймаут сигнала");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("Обвязка прервана", e);
        }
    }

    private static void counterTest(Mutex mutex, int threadCount) throws Exception {
        Counter counter = new Counter();
        AtomicInteger inside = new AtomicInteger();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        CountDownLatch start = new CountDownLatch(1);
        List<Thread> threads = new ArrayList<>();
        int iterations = 10_000;
        for (int i = 0; i < threadCount; i++) {
            threads.add(launch("counter-" + i, failure, () -> {
                await(start);
                for (int j = 0; j < iterations; j++) {
                    mutex.lock();
                    try {
                        int count = inside.incrementAndGet();
                        try {
                            require(count == 1, "Одновременно вошли несколько потоков");
                            long old = counter.value;
                            if ((j & 255) == 0) Thread.yield();
                            counter.value = old + 1;
                        } finally { inside.decrementAndGet(); }
                    } finally { mutex.unlock(); }
                }
            }));
        }
        start.countDown();
        joinAll(threads, failure);
        require(counter.value == (long) threadCount * iterations, "Потеря обновлений");
        System.out.println("OK: счётчик=" + counter.value + ", потоков=" + threadCount);
    }

    private static void ownerTest(Mutex mutex) throws Exception {
        AtomicReference<Throwable> failure = new AtomicReference<>();
        mutex.lock();
        try {
            Thread stranger = launch("stranger", failure, () -> {
                boolean rejected = false;
                try { mutex.unlock(); }
                catch (IllegalMonitorStateException expected) { rejected = true; }
                require(rejected, "Чужой unlock должен быть отклонён");
            });
            joinAll(List.of(stranger), failure);
        } finally { mutex.unlock(); }
        boolean rejected = false;
        try { mutex.unlock(); }
        catch (IllegalMonitorStateException expected) { rejected = true; }
        require(rejected, "Повторный unlock свободного замка должен быть отклонён");
        System.out.println("OK: проверка владельца");
    }

    private static void nestedTest(Mutex mutex) throws Exception {
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread t = launch("nested", failure, () -> {
            mutex.lock();
            try {
                mutex.lock();
                try { /* Вложенная критическая секция. */ }
                finally { mutex.unlock(); }
            } finally { mutex.unlock(); }
        });
        joinAll(List.of(t), failure);
        System.out.println("OK: вложенный захват и полное освобождение");
        counterTest(mutex, 2);
    }

    private static void partialReleaseTest(Mutex mutex) throws Exception {
        AtomicReference<Throwable> failure = new AtomicReference<>();
        CountDownLatch attempting = new CountDownLatch(1);
        CountDownLatch entered = new CountDownLatch(1);
        mutex.lock();
        Thread competitor;
        try {
            mutex.lock();
            try {
                competitor = launch("partial-release", failure, () -> {
                    attempting.countDown();
                    mutex.lock();
                    try { entered.countDown(); }
                    finally { mutex.unlock(); }
                });
                await(attempting);
            } finally { mutex.unlock(); }
            require(!entered.await(50, TimeUnit.MILLISECONDS),
                    "Промежуточный unlock освободил реентерабельный замок");
        } finally { mutex.unlock(); }
        joinAll(List.of(competitor), failure);
        require(entered.getCount() == 0, "Последний unlock не освободил замок");
        System.out.println("OK: промежуточный unlock сохраняет владение");
    }

    private static void nonReentrantTest(Mutex mutex) {
        mutex.lock();
        try {
            boolean rejected = false;
            try { mutex.lock(); }
            catch (IllegalStateException expected) { rejected = true; }
            require(rejected, "Повторный вход должен быть явно отклонён");
        } finally { mutex.unlock(); }
        System.out.println("OK: повторный вход отклонён");
    }

    private static void interruptTest(Mutex mutex) throws Exception {
        AtomicReference<Throwable> failure = new AtomicReference<>();
        CountDownLatch attempting = new CountDownLatch(1);
        CountDownLatch acquired = new CountDownLatch(1);
        mutex.lock();
        Thread waiter;
        try {
            waiter = launch("interrupted-waiter", failure, () -> {
                attempting.countDown();
                mutex.lock();
                try {
                    require(Thread.currentThread().isInterrupted(), "Статус interrupt потерян");
                    acquired.countDown();
                } finally { mutex.unlock(); }
            });
            await(attempting);
            waiter.interrupt();
            require(!acquired.await(50, TimeUnit.MILLISECONDS), "Вход при занятом замке");
        } finally { mutex.unlock(); }
        joinAll(List.of(waiter), failure);
        require(acquired.getCount() == 0, "Ожидающий не получил замок");
        System.out.println("OK: interrupt не отменил ожидание, статус сохранён");
    }

    private record WaitingResult(String mode, long wallNanos, long cpuNanos) {}

    private static WaitingResult waitingSample(boolean spin, long millis) throws Exception {
        ThreadMXBean bean = ManagementFactory.getThreadMXBean();
        boolean cpuAvailable = bean.isCurrentThreadCpuTimeSupported();
        if (cpuAvailable && !bean.isThreadCpuTimeEnabled()) {
            try { bean.setThreadCpuTimeEnabled(true); }
            catch (UnsupportedOperationException | SecurityException e) { cpuAvailable = false; }
        }
        final boolean measureCpu = cpuAvailable && bean.isThreadCpuTimeEnabled();
        AtomicBoolean released = new AtomicBoolean();
        AtomicReference<WaitingResult> result = new AtomicReference<>();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        CountDownLatch ready = new CountDownLatch(1);
        Thread waiter = launch("demo-" + (spin ? "spin" : "park"), failure, () -> {
            long cpuStart = measureCpu ? bean.getCurrentThreadCpuTime() : -1;
            long start = System.nanoTime();
            ready.countDown();
            while (!released.get()) {
                if (spin) Thread.onSpinWait();
                else LockSupport.park(released);
            }
            long wall = System.nanoTime() - start;
            long cpuEnd = measureCpu ? bean.getCurrentThreadCpuTime() : -1;
            result.set(new WaitingResult(spin ? "spin" : "park", wall,
                    cpuStart >= 0 && cpuEnd >= 0 ? cpuEnd - cpuStart : -1));
        });
        await(ready);
        try { Thread.sleep(millis); }
        finally {
            released.set(true);
            LockSupport.unpark(waiter);
        }
        joinAll(List.of(waiter), failure);
        return result.get();
    }

    private static void waitingDemo(long millis) throws Exception {
        require(millis >= 50 && millis <= 3000, "Длительность: от 50 до 3000 ms");
        System.out.println("Один ожидающий поток; разрешение продолжить через " + millis + " ms.");
        for (int round = 0; round < 2; round++) {
            for (boolean spin : new boolean[] {true, false}) {
                WaitingResult r = waitingSample(spin, millis);
                String cpu = r.cpuNanos < 0 ? "недоступно" : String.format("%.2f ms", r.cpuNanos / 1e6);
                System.out.printf("%s: wall=%.2f ms, CPU=%s%n", r.mode, r.wallNanos / 1e6, cpu);
            }
        }
        System.out.println("Сравните расход CPU, не небольшие различия wall time.");
    }

    private static void runStep(String step) throws Exception {
        switch (step) {
            case "spin" -> {
                Mutex m = new SpinMutex();
                ownerTest(m); nonReentrantTest(m); counterTest(m, 4);
            }
            case "reentrant" -> {
                Mutex m = new ReentrantSpinMutex();
                ownerTest(m); nestedTest(m); partialReleaseTest(m); counterTest(m, 4);
            }
            case "parked" -> {
                Mutex m = new OneWaiterMutex();
                ownerTest(m); nonReentrantTest(m); counterTest(m, 2); interruptTest(m);
            }
            case "queue" -> {
                Mutex m = new QueuedMutex();
                ownerTest(m); nonReentrantTest(m); counterTest(m, 6); interruptTest(m);
            }
            default -> throw new IllegalArgumentException("Неизвестный шаг: " + step);
        }
    }

    public static void main(String[] args) throws Exception {
        String command = args.length == 0 ? "help" : args[0];
        if (command.equals("help")) {
            System.out.println("Задания и инструкции находятся в комментарии в начале файла.");
            System.out.println("java MutexWorkshop.java demo [50..3000 ms]");
            System.out.println("java MutexWorkshop.java spin|reentrant|parked|queue|all");
            return;
        }
        if (command.equals("demo")) {
            waitingDemo(args.length > 1 ? Long.parseLong(args[1]) : 500);
            return;
        }
        // Инициализируем используемый для парковки класс до начала упражнений.
        LockSupport.unpark(null);
        if (command.equals("all")) {
            for (String step : new String[] {"spin", "reentrant", "parked", "queue"}) {
                System.out.println("=== " + step + " ===");
                runStep(step);
            }
        } else runStep(command);
    }
}