package nl.pancompany.eventstore.record;

/**
 * @param startingPosition Start position, inclusive, possible range is [0, {@literal <last-position>}]
 * @param stopPosition     Stop position, exclusive, possible range is [0, {@literal <last-position+1>}]
 */
public record ReadOptions(SequencePosition startingPosition, SequencePosition stopPosition) {

    public static ReadOptionsBuilder builder() {
        return new ReadOptionsBuilder();
    }

    public static class ReadOptionsBuilder {

        private SequencePosition startingPosition = SequencePosition.of(0);
        private SequencePosition stopPosition;

        private ReadOptionsBuilder() {
        }

        /**
         * @param startingPosition Start position, inclusive, possible range is [0, {@literal <last-position>}], Defaults to 0
         * @return
         */
        public ReadOptionsBuilder withStartingPosition(int startingPosition) {
            return withStartingPosition(SequencePosition.of(startingPosition));
        }

        /**
         * @param startingPosition Start position, inclusive, possible range is [0, {@literal <last-position>}], Defaults to 0
         * @return
         */
        public ReadOptionsBuilder withStartingPosition(SequencePosition startingPosition) {
            this.startingPosition = startingPosition;
            return this;
        }

        /**
         * @param stopPosition Stopping position, exclusive, possible range is [0, {@literal <last-position+1>}], Defaults to null (no stopping position)
         * @return
         */
        public ReadOptionsBuilder withStoppingPosition(int stopPosition) {
            return withStoppingPosition(SequencePosition.of(stopPosition));
        }

        /**
         * @param stopPosition Stopping position, exclusive, possible range is [0, {@literal <last-position+1>}], Defaults to null (no stopping position)
         * @return
         */
        public ReadOptionsBuilder withStoppingPosition(SequencePosition stopPosition) {
            this.stopPosition = stopPosition;
            return this;
        }

        public ReadOptions build() {
            return new ReadOptions(this.startingPosition, this.stopPosition);
        }

    }

}
