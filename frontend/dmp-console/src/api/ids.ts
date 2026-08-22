/**
 * A shortened id that still identifies the thing.
 *
 * <p>Ids here are UUIDv7, whose leading bits are a timestamp. Truncating to the first eight
 * characters therefore keeps only the part that is guaranteed to collide: two runs created in the
 * same few milliseconds render identically. That is not a cosmetic problem — a support engineer
 * comparing a record's WRITTEN entry against its REJECTED entry saw "same run, same chunk" and
 * reasonably concluded the platform had contradicted itself, when they were two different runs.
 *
 * <p>Keeping a tail restores the distinguishing part, since the low bits of a v7 are random.
 */
export function shortId(id: string | null | undefined): string {
  if (!id) return '—'
  return id.length <= 13 ? id : `${id.slice(0, 8)}…${id.slice(-4)}`
}
