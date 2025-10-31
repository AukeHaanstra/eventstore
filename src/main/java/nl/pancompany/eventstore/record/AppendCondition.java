package nl.pancompany.eventstore.record;

import nl.pancompany.eventstore.EventStore;
import nl.pancompany.eventstore.query.Query;

public record AppendCondition(Query failIfEventsMatch, EventStore.SequencePosition after) {

    public static AppendConditionBuilder builder() {
        return new AppendConditionBuilder();
    }

    public static class AppendConditionBuilder {
        private Query failIfEventsMatch;
        private EventStore.SequencePosition after;

        private AppendConditionBuilder() {
        }

        public AppendConditionBuilder.FailIfEventsMatchAfterBuilder failIfEventsMatch(Query failIfEventsMatch) {
            this.failIfEventsMatch = failIfEventsMatch;
            return this.new FailIfEventsMatchAfterBuilder();
        }

        public class FailIfEventsMatchAfterBuilder {

            public AppendConditionBuilder after(int sequencePosition) {
                AppendConditionBuilder.this.after = EventStore.SequencePosition.of(sequencePosition);
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
