#include <stdint.h>
#include <stdlib.h>

#define DAY_MS INT64_C(86400000)
#define LOOKBACK_MS (7 * DAY_MS)

enum { SESSIONS, PURCHASING_SESSIONS, REVENUE, ASSISTS, METRIC_COUNT };

struct interval {
    int64_t first_click;
    int64_t last_click;
    int active;
};

static int64_t utc_day(int64_t ms) {
    return ms / DAY_MS - (ms % DAY_MS < 0);
}

static size_t lower_bound(const int64_t *times, size_t first, size_t end, int64_t target) {
    while (first < end) {
        size_t mid = first + (end - first) / 2;
        if (times[mid] < target) first = mid + 1;
        else end = mid;
    }
    return first;
}

static void count_assists(const int64_t *times, size_t first, size_t end,
                          struct interval interval, int64_t first_day, int64_t *totals) {
    size_t begin = lower_bound(times, first, end, interval.first_click);
    size_t stop = lower_bound(times, begin, end, interval.last_click + LOOKBACK_MS + 1);
    while (begin < stop) {
        int64_t day = utc_day(times[begin]);
        size_t next = utc_day(times[stop - 1]) == day
            ? stop : lower_bound(times, begin + 1, stop, (day + 1) * DAY_MS);
        totals[(day - first_day) * METRIC_COUNT + ASSISTS] += (int64_t)(next - begin);
        begin = next;
    }
}

/* Each group is sorted by person and time. At equal click times the smallest
   ID is last. The final campaign index is reserved for organic traffic. */
int attribute(
    size_t session_count, const uint32_t *session_person, const int64_t *session_time,
    const int64_t *session_revenue, size_t click_count, const uint32_t *click_person,
    const int64_t *click_time, const uint32_t *click_campaign, uint32_t organic,
    int64_t first_day, size_t day_count, int64_t *totals
) {
    size_t stride = day_count * METRIC_COUNT;
    struct interval *intervals = calloc((size_t)organic + 1, sizeof(*intervals));
    uint32_t *touched = malloc(((size_t)organic + 1) * sizeof(*touched));
    if (!intervals || !touched) {
        free(intervals);
        free(touched);
        return 1;
    }

    size_t click = 0;
    for (size_t first = 0; first < session_count;) {
        uint32_t person = session_person[first];
        size_t end = first + 1;
        while (end < session_count && session_person[end] == person) ++end;
        while (click < click_count && click_person[click] < person) ++click;
        size_t click_begin = click;
        size_t touched_count = 0;
        while (click < click_count && click_person[click] == person) {
            uint32_t campaign = click_campaign[click];
            int64_t time = click_time[click++];
            struct interval *interval = &intervals[campaign];
            if (!interval->active) {
                interval->active = 1;
                interval->first_click = time;
                touched[touched_count++] = campaign;
            } else if (time > interval->last_click + LOOKBACK_MS) {
                count_assists(session_time, first, end, *interval, first_day,
                              totals + campaign * stride);
                interval->first_click = time;
            }
            interval->last_click = time;
        }
        for (size_t i = 0; i < touched_count; ++i) {
            uint32_t campaign = touched[i];
            count_assists(session_time, first, end, intervals[campaign], first_day,
                          totals + campaign * stride);
            intervals[campaign].active = 0;
        }

        size_t eligible = click_begin;
        for (size_t i = first; i < end; ++i) {
            int64_t time = session_time[i];
            while (eligible < click && click_time[eligible] <= time) ++eligible;
            uint32_t campaign = organic;
            if (eligible > click_begin && time - click_time[eligible - 1] <= LOOKBACK_MS) {
                campaign = click_campaign[eligible - 1];
            }
            size_t day = (size_t)(utc_day(time) - first_day);
            int64_t *row = totals + campaign * stride + day * METRIC_COUNT;
            ++row[SESSIONS];
            row[PURCHASING_SESSIONS] += session_revenue[i] > 0;
            row[REVENUE] += session_revenue[i];
        }
        first = end;
    }
    free(touched);
    free(intervals);
    return 0;
}
