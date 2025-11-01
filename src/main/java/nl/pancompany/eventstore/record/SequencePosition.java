package nl.pancompany.eventstore.record;

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
