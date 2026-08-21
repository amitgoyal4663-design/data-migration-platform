package com.dmp.persistence.mongo;

import com.dmp.domain.run.RunId;
import com.dmp.domain.run.RunMetrics;
import com.dmp.domain.tenant.TenantId;
import com.dmp.persistence.mongo.adapter.RunRepositoryAdapter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.data.mongodb.core.query.UpdateDefinition;

import java.lang.reflect.RecordComponent;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;

/**
 * Every counter on {@link RunMetrics} must actually be incremented.
 *
 * <p>The increment is a hand-written list of field names, and adding a counter to the record does
 * not add it to that list. When {@code recordsProduced} was introduced, the engine counted it, the
 * checkpoint stored it, the API exposed it — and the run document stayed at zero, because one line
 * in the adapter was missing. Nothing failed; the number was simply wrong.
 *
 * <p>So this test reads the components off the record rather than naming them, and fails the
 * moment a new one is added without a matching increment.
 *
 * <p>It once exempted {@code splitsTotal}, on the correct reasoning that a planned run knows its
 * total up front and incrementing it per chunk would multiply it. Lazy chunking removed that
 * premise — a sequential run generates one chunk at a time and grows the total as it goes — and the
 * exemption outlived the reason for it, turning a guard into cover for the same bug it was written
 * to catch. Runs of twenty-one chunks reported one. There are no exemptions now: a caller that is
 * not extending the plan passes zero, and the adapter's own non-zero guard drops it.
 */
@ExtendWith(MockitoExtension.class)
class MetricIncrementCoverageTest {

    @Mock private MongoTemplate mongo;

    @Test
    void everyCounterOnRunMetricsIsIncremented() {
        RunRepositoryAdapter adapter = new RunRepositoryAdapter(mongo);

        // Every component non-zero, so nothing is skipped by the adapter's own "only if non-zero"
        // guard and the update must mention them all.
        RunMetrics delta = new RunMetrics(1, 1, 1, 1, 1, 1, 1, 1, 1);

        adapter.incrementMetrics(TenantId.newId(), RunId.newId(), delta);

        ArgumentCaptor<UpdateDefinition> captor = ArgumentCaptor.forClass(UpdateDefinition.class);
        verify(mongo).updateFirst(any(Query.class), captor.capture(), any(Class.class));

        String increments = ((Update) captor.getValue()).getUpdateObject().toString();

        List<String> expected = Arrays.stream(RunMetrics.class.getRecordComponents())
                .map(RecordComponent::getName)
                .toList();

        assertThat(expected).isNotEmpty();
        assertThat(increments)
                .as("every RunMetrics counter must appear in the $inc; a missing one silently "
                        + "reports zero for that metric on every run")
                .contains(expected.stream().map(name -> "metrics." + name).toList());
    }
}
