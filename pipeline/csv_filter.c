#define _GNU_SOURCE
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Validate the task's intentionally narrow CSV dialect, keeping only complete
   rows. Newline is the sole record delimiter; a preceding CR is field data. */
static int emit_row(FILE **files, size_t count, int key, char *line, size_t len,
                    int expected, char *output) {
    if (len && line[len - 1] == '\n') --len;
    if (len == 0) return 0;
    char *starts[16];
    size_t sizes[16];
    size_t begin = 0;
    for (int i = 0; i < expected; ++i) {
        char *start = line + begin;
        char *comma = memchr(start, ',', len - begin);
        if ((comma != NULL) != (i + 1 < expected)) return 0;
        size_t size = comma ? (size_t)(comma - start) : len - begin;
        begin += size + 1;
        if (size && *start == '"') {
            if (size < 2 || start[size - 1] != '"') return 0;
            ++start;
            size -= 2;
        }
        if (memchr(start, '"', size)) return 0;
        starts[i] = start;
        sizes[i] = size;
    }
    size_t bucket = 0;
    if (count > 1) {
        uint64_t hash = UINT64_C(14695981039346656037);
        for (size_t i = 0; i < sizes[key]; ++i) {
            hash ^= (unsigned char)starts[key][i];
            hash *= UINT64_C(1099511628211);
        }
        bucket = hash % count;
    }
    char *end = output;
    for (int i = 0; i < expected; ++i) {
        *end++ = '"';
        memcpy(end, starts[i], sizes[i]);
        end += sizes[i];
        *end++ = '"';
        *end++ = i + 1 < expected ? ',' : '\n';
    }
    size_t size = (size_t)(end - output);
    return fwrite(output, 1, size, files[bucket]) == size ? 0 : -1;
}

int main(int argc, char **argv) {
    if (argc < 6) return 2;
    int expected = atoi(argv[1]);
    int count = atoi(argv[4]), key = atoi(argv[5]);
    if (expected < 1 || expected > 16 || count < 1 || count > 128 ||
        key < 0 || key >= expected) return 2;
    FILE *files[128];
    for (int i = 0; i < count; ++i) {
        char *path;
        if (asprintf(&path, "%s-%03d.csv", argv[3], i) < 0) return 3;
        files[i] = fopen(path, "wb");
        free(path);
        if (!files[i]) return 3;
        setvbuf(files[i], NULL, _IOFBF, 1 << 20);
        if (fprintf(files[i], "%s\n", argv[2]) < 0) return 5;
    }
    char *line = NULL;
    char *output = NULL;
    size_t cap = 0, output_cap = 0;
    for (int a = 6; a < argc; ++a) {
        FILE *in = fopen(argv[a], "rb");
        if (!in) return 4;
        setvbuf(in, NULL, _IOFBF, 1 << 20);
        if (getline(&line, &cap, in) >= 0) {
            ssize_t n;
            while ((n = getline(&line, &cap, in)) >= 0) {
                if (output_cap < cap + 2 * (size_t)expected + 1) {
                    output_cap = cap + 2 * (size_t)expected + 1;
                    char *buffer = realloc(output, output_cap);
                    if (!buffer) return 3;
                    output = buffer;
                }
                if (emit_row(files, count, key, line, (size_t)n, expected, output) < 0)
                    return 5;
            }
        }
        if (ferror(in)) return 4;
        fclose(in);
    }
    free(line);
    free(output);
    for (int i = 0; i < count; ++i)
        if (fclose(files[i]) != 0) return 5;
    return 0;
}
