package com.psddev.dari.db;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.psddev.dari.util.ObjectUtils;
import com.psddev.dari.util.PaginatedResult;

/**
 * Aggregates multiple queries into a single paginated result.
 *
 * <p>For example:</p>
 *
 * <blockquote><pre>{@literal
Query<Author> authorQuery = Query.from(Author.class).where(...);
Query<Article> articleQuery = Query.from(Article.class).where(...);
PaginatedResult<Object> result = new AggregateQueryResult<Object>(10L, 10, authorQuery, articleQuery);
 * }</pre></blockquote>
 *
 * <p>If these queries return 3 and 17 items respectively, the {@code result}
 * would contain 3 authors and 7 articles.</p>
 */
public class AggregateQueryResult<E> extends PaginatedResult<E> {

    private final Iterator<Query<? extends E>> queriesIterator;
    private final List<Query<? extends E>> queries = new ArrayList<Query<? extends E>>();
    private final Map<Query<? extends E>, Long> counts = new HashMap<Query<? extends E>, Long>();

    private Long aggregateCount;
    private List<E> items;
    private Boolean hasNext;

    /**
     * Creates an instance with the given {@code offset}, {@code limit},
     * and {@code queriesIterator}.
     */
    public AggregateQueryResult(long offset, int limit, Iterator<Query<? extends E>> queriesIterator) {
        super(offset, limit, -1L, null);
        this.queriesIterator = queriesIterator;
    }

    /**
     * Creates an instance with the given {@code offset}, {@code limit},
     * and {@code queriesIterable}.
     */
    public AggregateQueryResult(long offset, int limit, Iterable<Query<? extends E>> queriesIterable) {
        this(offset, limit, queriesIterable.iterator());
    }

    /**
     * Creates an instance with the given {@code offset}, {@code limit},
     * and {@code queries}.
     *
     * @param queries {@code null} is equivalent to an empty array.
     */
    @SuppressWarnings("unchecked")
    public AggregateQueryResult(long offset, int limit, Query<? extends E>... queries) {
        this(offset, limit, ObjectUtils.to(Iterable.class, queries));
    }

    // --- PaginatedResult support ---

    private Query<? extends E> getQueryFor(int index) {
        if (index < queries.size()) {
            return queries.get(index);

        } else {
            while (index >= queries.size() && queriesIterator.hasNext()) {
                queries.add(queriesIterator.next());
            }

            return index < queries.size() ? queries.get(index) : null;
        }
    }

    private Long getCountFor(int index) {
        Query<? extends E> query = getQueryFor(index);

        if (query == null) {
            return null;

        } else {
            Long count = counts.get(query);

            if (count == null) {
                count = query.count();
                counts.put(query, count);
            }

            return count;
        }
    }

    @Override
    public long getCount() {
        if (aggregateCount == null) {
            aggregateCount = 0L;

            for (int i = 0;; ++ i) {
                Long count = getCountFor(i);

                if (count == null) {
                    break;
                }

                aggregateCount += count;
            }
        }

        return aggregateCount;
    }

    @Override
    public List<E> getItems() {
        if (items == null) {
            items = new ArrayList<E>();

            long start = getOffset();
            long end = start + getLimit();
            long current = 0;

            for (int i = 0;; ++ i) {
                Long count = getCountFor(i);

                if (count == null) {
                    break;
                }

                long next = current + count;

                if (current < end && next > start) {
                    items.addAll(getQueryFor(i).select(
                            start <= current ? 0L : (start - current),
                            (int) (end - current)).getItems());

                    if (next >= end) {
                        break;
                    }
                }

                current = next;
            }
        }

        return items;
    }

    @Override
    public boolean hasNext() {
        if (hasNext == null) {
            hasNext = false;

            long nextCount = 0L;
            long nextOffset = getNextOffset();

            for (int i = 0;; ++ i) {
                Long count = getCountFor(i);

                if (count == null) {
                    break;
                }

                nextCount += count;

                if (nextCount > nextOffset) {
                    hasNext = true;
                    break;
                }
            }
        }

        return hasNext;
    }
}
