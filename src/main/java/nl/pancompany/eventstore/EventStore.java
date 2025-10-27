package nl.pancompany.eventstore;

import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

// TODO: Passage-of-time events https://verraes.net/2019/05/patterns-for-decoupling-distsys-passage-of-time-event/
@Slf4j
public class EventStore {

    private final List<SequencedEvent> storedEvents = new ArrayList<>();
    private final Map<Tag, Set<SequencePosition>> tagPositions = new HashMap<>();
    private final Map<Type, Set<SequencePosition>> typePositions = new HashMap<>();
    private final Set<SequencePosition> allSequencePositions = new HashSet<>();
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final Lock readLock = lock.readLock();
    private final Lock writeLock = lock.writeLock();
    private final Map<Type, Set<InvocableMethod>> synchronousEventHandlers = new HashMap<>();
    private final Map<Type, Set<InvocableMethod>> asynchronousEventHandlers = new HashMap<>();
    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private final Set<InvocableMethod> synchronousResetHandlers = new HashSet<>();
    private final Set<InvocableMethod> asynchronousResetHandlers = new HashSet<>();

    public StateManager getStateManager() {
        return new StateManager(this);
    }

    public void registerSynchronousEventHandlers(Class<?> eventHandlerClass) {
        Object eventHandlerInstance = getInstance(eventHandlerClass);
        registerResetHandler(eventHandlerClass, eventHandlerInstance, true);
        registerEventHandler(eventHandlerClass, eventHandlerInstance, true);
    }

    public void registerAsynchronousEventHandlers(Class<?> eventHandlerClass) {
        Object eventHandlerInstance = getInstance(eventHandlerClass);
        registerResetHandler(eventHandlerClass, eventHandlerInstance, false);
        registerEventHandler(eventHandlerClass, eventHandlerInstance, false);
    }

    public void registerSynchronousEventHandlers(Object eventHandlerInstance) {
        registerResetHandler(eventHandlerInstance.getClass(), eventHandlerInstance, true);
        registerEventHandler(eventHandlerInstance.getClass(), eventHandlerInstance, true);
    }

    public void registerAsynchronousEventHandlers(Object eventHandlerInstance) {
        registerResetHandler(eventHandlerInstance.getClass(), eventHandlerInstance, false);
        registerEventHandler(eventHandlerInstance.getClass(), eventHandlerInstance, false);
    }

    private void registerResetHandler(Class<?> eventHandlerClass, Object instance, boolean synchronous) {
        Set<Method> resetHandlerMethods = Arrays.stream(eventHandlerClass.getDeclaredMethods()).filter(
                method -> method.isAnnotationPresent(ResetHandler.class)).collect(Collectors.toSet());
        if (resetHandlerMethods.size() > 1) {
            throw new IllegalArgumentException("Multiple reset handlers are not allowed.");
        }
        resetHandlerMethods.forEach(method -> method.setAccessible(true));
        Set<InvocableMethod> resetHandlers = synchronous ? synchronousResetHandlers : asynchronousResetHandlers;
        resetHandlerMethods.stream().findFirst().ifPresent(method -> resetHandlers.add(new InvocableMethod(instance, method)));
    }

    private void registerEventHandler(Class<?> eventHandlerClass, Object instance, boolean synchronous) {
        Set<Method> eventHandlerMethods = Arrays.stream(eventHandlerClass.getDeclaredMethods()).filter(
                method -> method.isAnnotationPresent(EventHandler.class)).collect(Collectors.toSet());
        eventHandlerMethods.forEach(method -> method.setAccessible(true));
        Map<Type, InvocableMethod> newEventHandlers = eventHandlerMethods.stream().collect(Collectors.toMap(
                this::getEventType,
                method -> new InvocableMethod(instance, method)
        ));
        Map<Type, Set<InvocableMethod>> eventHandlers = synchronous ? synchronousEventHandlers : asynchronousEventHandlers;
        newEventHandlers.keySet().forEach(key -> eventHandlers.computeIfAbsent(key, type -> new HashSet<>())
                .add(newEventHandlers.get(key)));
    }

    private static Object getInstance(Class<?> eventHandlerClass) {
        try {
            Constructor<?> noArgConstructor = eventHandlerClass.getDeclaredConstructors()[0];
            noArgConstructor.setAccessible(true);
            return noArgConstructor.newInstance();
        } catch (InstantiationException | InvocationTargetException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    private Type getEventType(Method eventHandlerMethod) {
        Class<?> declaredParemeterType = eventHandlerMethod.getParameters()[0].getType();
        EventHandler annotation = eventHandlerMethod.getAnnotation(EventHandler.class);
        if (declaredParemeterType == Object.class && annotation.type().isBlank()) {
            throw new IllegalArgumentException("EventHandler annotation must have a type defined when the first parameter is Object.");
        } else if (declaredParemeterType != Object.class && !annotation.type().isBlank()) {
            throw new IllegalArgumentException("Either declare an @EventHandler(type = ..) with an Object parameter," +
                    "or declare @EventHandler with a typed parameter.");
        } else if (declaredParemeterType == Object.class) {
            return Type.of(annotation.type());
        }
        return Type.of(declaredParemeterType);
    }

    /**
     * Contract: event payload must always be immutable to guarantee immutability of events in the event store
     *
     * @param events The event instances that wrap a payload (the raw event).
     */
    public void append(Event... events) {
        try {
            append(List.of(events), null);
        } catch (AppendConditionNotSatisfied e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * Contract: event payload must always be immutable to guarantee immutability of events in the event store
     *
     * @param events The event instances that wrap a payload (the raw event).
     */
    public void append(List<Event> events) {
        try {
            append(events, null);
        } catch (AppendConditionNotSatisfied e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * Contract: event payload must always be immutable to guarantee immutability of events in the event store
     *
     * @param event The event instances that wrap a payload (the raw event).
     */
    public void append(Event event, AppendCondition appendCondition) throws AppendConditionNotSatisfied {
        append(List.of(event), appendCondition);
    }

    /**
     * Contract: event payload must always be immutable to guarantee immutability of events in the event store
     *
     * @param events The event instances that wrap a payload (the raw event).
     */
    public void append(List<Event> events, AppendCondition appendCondition) throws AppendConditionNotSatisfied {
        try {
            writeLock.lock();
            if (appendCondition != null) {
                checkWhetherEventsFailAppendCondition(appendCondition);
            }
            for (Event event : events) {
                SequencePosition insertPosition = SequencePosition.of(storedEvents.size());
                SequencedEvent storedEvent = new SequencedEvent(event, insertPosition);
                storedEvents.add(storedEvent);
                allSequencePositions.add(insertPosition);
                for (Tag tag : event.tags) {
                    tagPositions.computeIfAbsent(tag, k -> new HashSet<>()).add(insertPosition); // add to tag-index
                }
                typePositions.computeIfAbsent(event.type, k -> new HashSet<>()).add(insertPosition); // add to type-index
                invokeEventHandlers(storedEvent);
            }
        } finally {
            writeLock.unlock();
        }

    }

    /**
     * Resets and replays events to registered event handlers.
     *
     * @param end end position, exclusive
     */
    public void replay(SequencePosition end) {
        try {
            readLock.lock();
            synchronousResetHandlers.forEach(EventStore::invoke);
            asynchronousResetHandlers.forEach(resetHandler -> executor.submit(() -> invoke(resetHandler)));
            storedEvents.subList(0, end.value).forEach(this::invokeEventHandlers);
        } finally {
            readLock.unlock();
        }
    }

    private static void invoke(InvocableMethod instance) {
        try {
            instance.method().invoke(instance.objectWithMethod());
        } catch (IllegalAccessException e) {
            log.warn("Could not invoke reset handler method.", e);
        } catch (InvocationTargetException e) {
            log.warn("Invoked reset handler threw exception", e);
        }
    }

    private void invokeEventHandlers(SequencedEvent sequencedEvent) {
        if (asynchronousEventHandlers.containsKey(sequencedEvent.type)) {
            executor.submit(() -> asynchronousEventHandlers.get(sequencedEvent.type)
                    .forEach(instance -> invoke(instance, sequencedEvent)));
        }
        if (synchronousEventHandlers.containsKey(sequencedEvent.type)) {
            synchronousEventHandlers.get(sequencedEvent.type)
                    .forEach(instance -> invoke(instance, sequencedEvent));
        }
    }

    private static void invoke(InvocableMethod instance, SequencedEvent event) {
        if (instance == null) {
            return; // skip event if no handler
        }
        try {
            instance.method().invoke(instance.objectWithMethod(), event.payload());
        } catch (IllegalAccessException e) {
            log.warn("Could not invoke handler method for event {}", event, e);
        } catch (InvocationTargetException e) {
            log.warn("Invoked handler threw exception for event {}", event, e);
        }
    }

    private void checkWhetherEventsFailAppendCondition(AppendCondition appendCondition) throws AppendConditionNotSatisfied {
        List<SequencedEvent> queryResult = queryEvents(
                appendCondition.failIfEventsMatch(),
                appendCondition.after() == null ? null : ReadOptions.builder()
                        .withStartingPosition(appendCondition.after().incrementAndGet().value()).build());
        if (!queryResult.isEmpty()) {
            if (appendCondition.after() == null) {
                throw new AppendConditionNotSatisfied("An event matched the provided failIfEventsMatch query");
            }
            throw new AppendConditionNotSatisfied("An event matched the provided failIfEventsMatch query after sequence number " + appendCondition.after());
        }
    }

    public List<SequencedEvent> read(Query query) {
        return read(query, null);
    }

    public List<SequencedEvent> read(Query query, ReadOptions options) {
        try {
            readLock.lock();
            return queryEvents(query, options);
        } finally {
            readLock.unlock();
        }
    }

    private List<SequencedEvent> queryEvents(Query query, ReadOptions options) {
        Set<SequencePosition> sequencePositionsFromStart = allSequencePositions.stream().filter(options == null ?
                        position -> true :
                        position -> position.value >= options.startingPosition.value)
                .collect(Collectors.toSet()); // populate base set of positions to check
        Set<SequencePosition> querySequencePositions = new HashSet<>();
        for (QueryItem queryItem : query.getQueryItems()) {
            if (queryItem.isAll()) {
                return sequencePositionsToEvents(sequencePositionsFromStart); // just map base set to events
            }
            Set<SequencePosition> queryItemSequencePositions = new HashSet<>(sequencePositionsFromStart); // mutable base-set of positions
            if (!queryItem.isAllTags()) { // if all, then retain base-set, otherwise:
                for (Tag tag : queryItem.tags()) { // step-wise intersection with the set of positions for each query tag (AND)
                    queryItemSequencePositions.retainAll(tagPositions.computeIfAbsent(tag, t -> new HashSet<>()));
                }
            }
            if (!queryItem.isAllTypes()) { // if all, no second intersection, otherwise:
                Set<SequencePosition> queryItemTypePositions = new HashSet<>();
                for (Type type : queryItem.types()) { // step-wise union of the position sets of all query event types (OR)
                    queryItemTypePositions.addAll(typePositions.computeIfAbsent(type, t -> new HashSet<>()));
                }
                queryItemSequencePositions.retainAll(queryItemTypePositions); // intersection with the set of positions of all query event types (AND)
            }
            querySequencePositions.addAll(queryItemSequencePositions); // union of all sets of positions for all query items (OR)
        }
        return sequencePositionsToEvents(querySequencePositions);
    }

    private List<SequencedEvent> sequencePositionsToEvents(Set<SequencePosition> querySequencePositions) {
        return querySequencePositions.stream()
                .sorted()
                .map(position -> storedEvents.get(position.value))
                .toList();
    }

    public record Event(Object payload, Set<Tag> tags, Type type) {

        public Event(Object payload) {
            this(payload, Collections.emptySet(), getName(payload));
        }

        public Event(Object payload, Type type) {
            this(payload, Collections.emptySet(), type);
        }

        public Event(Object payload, Tag... tags) {
            this(payload, Set.of(tags), getName(payload));
        }

        public Event(Object payload, Set<Tag> tags) {
            this(payload, tags, getName(payload));
        }

        public Event(Object payload, String... tags) {
            this(payload, Arrays.stream(tags).map(Tag::of).collect(Collectors.toSet()), getName(payload));
        }

        public Event(Object payload, Type type, Tag... tags) {
            this(payload, Set.of(tags), type);
        }

        public Event(Object payload, Type type, Set<Tag> tags) {
            this(payload, tags, type);
        }

        public Event(Object payload, Type type, String... tags) {
            this(payload, Arrays.stream(tags).map(Tag::of).collect(Collectors.toSet()), type);
        }

        public static Event of(Object payload) {
            return new Event(payload, Collections.emptySet(), getName(payload));
        }

        public static Event of(Object payload, Type type) {
            return new Event(payload, Collections.emptySet(), type);
        }

        public static Event of(Object payload, Tag... tags) {
            return new Event(payload, Set.of(tags), getName(payload));
        }

        public static Event of(Object payload, Set<Tag> tags) {
            return new Event(payload, tags, getName(payload));
        }

        public static Event of(Object payload, String... tags) {
            return new Event(payload, Arrays.stream(tags).map(Tag::of).collect(Collectors.toSet()), getName(payload));
        }

        public static Event of(Object payload, Type type, Tag... tags) {
            return new Event(payload, Set.of(tags), type);
        }

        public static Event of(Object payload, Type type, Set<Tag> tags) {
            return new Event(payload, tags, type);
        }

        public static Event of(Object payload, Type type, String... tags) {
            return new Event(payload, Arrays.stream(tags).map(Tag::of).collect(Collectors.toSet()), type);
        }

        private static Type getName(Object payload) {
            return Type.of(payload.getClass());
        }

    }

    public record SequencedEvent(Object payload, Set<Tag> tags, Type type, SequencePosition position) {

        public SequencedEvent(Event event, SequencePosition position) {
            this(event.payload, event.tags, event.type, position);
        }

        @SuppressWarnings("unchecked")
        public <T> T payload(Class<T> clazz) {
            if (!clazz.isAssignableFrom(payload.getClass())) {
                throw new IllegalArgumentException("Payload is not assignable to " + clazz);
            }
            return (T) payload;
        }

        /**
         * Beware, this operation causes a loss of sequence position information.
         * @return The event corresponding to this sequenced event
         */
        public Event toEvent() {
            return new Event(payload(), tags, type);
        }
    }

    public record SequencePosition(int value) implements Comparable<SequencePosition> {

        public static SequencePosition of(int i) {
            return new SequencePosition(i);
        }

        public SequencePosition incrementAndGet() {
            return new SequencePosition(value + 1);
        }

        @Override
        public int compareTo(SequencePosition anotherSequencePosition) {
            return Integer.compare(this.value, anotherSequencePosition.value);
        }

    }

    public record AppendCondition(Query failIfEventsMatch, SequencePosition after) {

        public static AppendConditionBuilder builder() {
            return new AppendConditionBuilder();
        }

        public static class AppendConditionBuilder {
            private Query failIfEventsMatch;
            private SequencePosition after;

            private AppendConditionBuilder() {
            }

            public FailIfEventsMatchAfterBuilder failIfEventsMatch(Query failIfEventsMatch) {
                this.failIfEventsMatch = failIfEventsMatch;
                return this.new FailIfEventsMatchAfterBuilder();
            }

            public class FailIfEventsMatchAfterBuilder {

                public AppendConditionBuilder after(int sequencePosition) {
                    AppendConditionBuilder.this.after = SequencePosition.of(sequencePosition);
                    return AppendConditionBuilder.this;
                }

                public AppendCondition build() {
                    return AppendConditionBuilder.this.build();
                }
            }

            public AppendCondition build() {
                if (this.failIfEventsMatch == null) {
                    throw new IllegalArgumentException("failIfEventsMatch must be set");
                }
                return new AppendCondition(this.failIfEventsMatch, this.after);
            }
        }
    }

    /**
     * @param startingPosition Start position, inclusive, possible range is [0, {@literal <last-position>}]
     */
    public record ReadOptions(SequencePosition startingPosition) {

        public static ReadOptionsBuilder builder() {
            return new ReadOptionsBuilder();
        }

        public static class ReadOptionsBuilder {

            private SequencePosition startingPosition;

            private ReadOptionsBuilder() {
            }

            /**
             * @param startingPosition Start position, inclusive, possible range is [0, {@literal <last-position>}]
             * @return
             */
            public ReadOptionsBuilder withStartingPosition(int startingPosition) {
                this.startingPosition = SequencePosition.of(startingPosition);
                return this;
            }

            public ReadOptions build() {
                return new ReadOptions(this.startingPosition);
            }

        }

    }

    public class AppendConditionNotSatisfied extends Exception {

        public AppendConditionNotSatisfied(String message) {
            super(message);
        }
    }

}
