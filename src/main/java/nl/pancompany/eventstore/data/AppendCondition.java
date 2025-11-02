package nl.pancompany.eventstore.data;

import nl.pancompany.eventstore.query.Query;

public record AppendCondition(Query failIfEventsMatch, SequencePosition after) {

    public static AppendConditionBuilder builder() {
        return new AppendConditionBuilder();
    }

    public static class AppendConditionBuilder {
        private Query failIfEventsMatch;
        private SequencePosition after;

        private AppendConditionBuilder() {
        }

        public AppendConditionBuilder.FailIfEventsMatchAfterBuilder failIfEventsMatch(Query failIfEventsMatch) {
            this.failIfEventsMatch = failIfEventsMatch;
            return this.new FailIfEventsMatchAfterBuilder();
        }

        public class FailIfEventsMatchAfterBuilder {

            public AppendConditionBuilder after(int sequencePosition) {
                AppendConditionBuilder.this.after = SequencePosition.of(sequencePosition);
                return AppendConditionBuilder.this;
            }

            public AppendConditionBuilder after(SequencePosition sequencePosition) {
                AppendConditionBuilder.this.after = sequencePosition;
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
