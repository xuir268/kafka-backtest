package kbt.diff;

/**
 * One difference between a baseline record and a candidate record at the same
 * position in delivery order.
 *
 * FIELD: both records exist and {@code field} differs. baseline and candidate hold the values.
 * MISSING_IN_CANDIDATE: only the baseline has a record here. baseline holds its description.
 * EXTRA_IN_CANDIDATE: only the candidate has a record here. candidate holds its description.
 */
public record Divergence(Kind kind, int index, String field, String baseline, String candidate) {

    public enum Kind {
        FIELD,
        MISSING_IN_CANDIDATE,
        EXTRA_IN_CANDIDATE
    }
}
