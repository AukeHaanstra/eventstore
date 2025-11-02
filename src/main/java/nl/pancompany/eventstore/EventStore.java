package nl.pancompany.eventstore;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import nl.pancompany.eventstore.exception.AppendConditionNotSatisfied;
import nl.pancompany.eventstore.query.Query;
import nl.pancompany.eventstore.query.QueryItem;
import nl.pancompany.eventstore.query.Tag;
import nl.pancompany.eventstore.query.Type;
import nl.pancompany.eventstore.data.*;

import java.time.Clock;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

@Slf4j
public class EventStore implements AutoCloseable {

    private final List<SequencedEvent> storedEvents = new ArrayList<>();
    private final Map<Tag, Set<SequencePosition>> tagPositions = new HashMap<>();
    private final Map<Type, Set<SequencePosition>> typePositions = new HashMap<>();
    private final List<SequencePosition> allSequencePositions = new ArrayList<>();
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final Lock readLock = lock.readLock();
    private final Lock writeLock = lock.writeLock();
    @Getter
    private final EventBus eventBus = new EventBus(this);
    private final Queue<SequencedEvent> addedEvents = new ConcurrentLinkedQueue<>();
    private final Clock clock;

    public EventStore() {
        this.clock = Clock.systemDefaultZone();
    }

    public EventStore(Clock clock) {
        this.clock = clock;
    }

    @SuppressWarnings("unchecked")
    public <T> StateManager<T> loadState(T emptyStateInstance, Query query) {
        requireNonNull(emptyStateInstance);
        var stateManager = new StateManager<>(this, (Class<T>) emptyStateInstance.getClass(), query);
        stateManager.load(emptyStateInstance);
        return stateManager;
    }

    public <T> StateManager<T> loadState(Class<T> stateClass, Query query) {
        var stateManager = new StateManager<>(this, stateClass, query);
        stateManager.load();
        return stateManager;
    }

    /**
     * Contract: event payload must always be immutable to guarantee immutability of events in the event store
     *
     * @param events The event instances that wrap a payload (the raw event).
     */
    public Optional<SequencePosition> append(Event... events) {
        requireNonNull(events); // empty array is however allowed
        Arrays.stream(events).forEach(e -> requireNonNull(requireNonNull(e).payload()));
        try {
            return append(List.of(events), null);
        } catch (AppendConditionNotSatisfied e) { // no append condition, so irrelevant
            throw new IllegalStateException(e);
        }
    }

    /**
     * Contract: event payload must always be immutable to guarantee immutability of events in the event store
     *
     * @param events The event instances that wrap a payload (the raw event).
     */
    public Optional<SequencePosition> append(List<Event> events) {
        requireNonNull(events); // empty list is however allowed
        events.forEach(e -> requireNonNull(e.payload()));
        try {
            return append(events, null);
        } catch (AppendConditionNotSatisfied e) { // no append condition, so irrelevant
            throw new IllegalStateException(e);
        }
    }

    /**
     * Contract: event payload must always be immutable to guarantee immutability of events in the event store
     *
     * @param event The event instances that wrap a payload (the raw event).
     */
    public Optional<SequencePosition> append(Event event, AppendCondition appendCondition) throws AppendConditionNotSatisfied {
        requireNonNull(event.payload());
        return append(List.of(event), appendCondition);
    }

    /**
     * Contract: event payload must always be immutable to guarantee immutability of events in the event store
     *
     * @param events The event instances that wrap a payload (the raw event).
     */
    public Optional<SequencePosition> append(List<Event> events, AppendCondition appendCondition) throws AppendConditionNotSatisfied {
        requireNonNull(events); // empty list is however allowed
        events.forEach(e -> requireNonNull(requireNonNull(e).payload()));
        SequencePosition lastInsertPosition = null;
        try {
            writeLock.lock();
            if (appendCondition != null) {
                checkWhetherAppendConditionFails(events, appendCondition);
            }
            for (Event event : events) {
                lastInsertPosition = SequencePosition.of(storedEvents.size());
                Metadata metadata = Metadata.of("timestamp", Instant.now(clock).toString());
                SequencedEvent storedEvent = new SequencedEvent(event, lastInsertPosition, metadata);
                storedEvents.add(storedEvent);
                addedEvents.offer(storedEvent); // offer() and writeLock guarantee sequential filling of queue
                allSequencePositions.add(lastInsertPosition);
                for (Tag tag : event.tags()) {
                    tagPositions.computeIfAbsent(tag, k -> new HashSet<>()).add(lastInsertPosition); // add to tag-index
                }
                typePositions.computeIfAbsent(event.type(), k -> new HashSet<>()).add(lastInsertPosition); // add to type-index
            }
        } finally {
            writeLock.unlock();
        }
        synchronized (eventBus) { // poll() & synchronized guarantee sequential in-order processing of queue elements
            while (!addedEvents.isEmpty()) {
                eventBus.invokeEventHandlers(addedEvents.poll());
            }
        }
        return Optional.ofNullable(lastInsertPosition);
    }

    private void checkWhetherAppendConditionFails(List<Event> events, AppendCondition appendCondition) throws AppendConditionNotSatisfied {
        List<SequencedEvent> queryResult = queryEvents(
                appendCondition.failIfEventsMatch(),
                appendCondition.after() == null ? null : ReadOptions.builder()
                        .withStartingPosition(appendCondition.after().incrementAndGet().value()).build());
        if (!queryResult.isEmpty()) {
            if (appendCondition.after() == null) {
                throw new AppendConditionNotSatisfied(
                        """
                                Apply() failed for events:%n - %s
                                One or more events matched the provided failIfEventsMatch query.
                                Matching events:%n - %s""".formatted(
                                events.stream().map(Event::toString).collect(joining(format("%n -"))),
                                queryResult.stream().map(SequencedEvent::toString).collect(joining(format("%n -")))
                        ));
            }
            throw new AppendConditionNotSatisfied(
                    """
                            Apply() failed for events:%n - %s
                            One or more events matched the provided failIfEventsMatch query after sequence number %s.
                            Matching events:%n - %s-""".formatted(
                            events.stream().map(Event::toString).collect(joining(format("%n -"))),
                            appendCondition.after(), queryResult.stream().map(SequencedEvent::toString).collect(joining(format("%n -")))
                    ));
        }
    }

    public List<SequencedEvent> read(Query query) {
        requireNonNull(query);
        return read(query, null);
    }

    public List<SequencedEvent> read(Query query, ReadOptions options) {
        requireNonNull(query);
        try {
            readLock.lock();
            return queryEvents(query, options);
        } finally {
            readLock.unlock();
        }
    }

    private List<SequencedEvent> queryEvents(Query query, ReadOptions options) {
        Set<SequencePosition> sequencePositionsFromSelection = getSelectedSequencePositions(options);
        Set<SequencePosition> querySequencePositions = new HashSet<>();
        for (QueryItem queryItem : query.getQueryItems()) {
            if (queryItem.isAll()) {
                return sequencePositionsToEvents(sequencePositionsFromSelection); // just map base set to events
            }
            Set<SequencePosition> queryItemSequencePositions = new HashSet<>(sequencePositionsFromSelection); // mutable base-set of positions
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

    private Set<SequencePosition> getSelectedSequencePositions(ReadOptions options) {
        Set<SequencePosition> sequencePositionsFromSelection;
        if (options == null) {
            sequencePositionsFromSelection = new HashSet<>(allSequencePositions);
        } else {
            sequencePositionsFromSelection = new HashSet<>(allSequencePositions.subList(
                    options.startingPosition().value(),
                    options.stopPosition() == null ? allSequencePositions.size() : options.stopPosition().value()));
        }
        return sequencePositionsFromSelection;
    }

    private List<SequencedEvent> sequencePositionsToEvents(Set<SequencePosition> querySequencePositions) {
        return querySequencePositions.stream()
                .sorted()
                .map(position -> storedEvents.get(position.value()))
                .toList();
    }

    @Override
    public void close() {
        eventBus.close();
    }

}
