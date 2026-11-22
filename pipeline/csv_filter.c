#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Validate the task's intentionally narrow CSV dialect, keeping only complete
   rows. Newline is the sole record delimiter; a preceding CR is field data. */
static int emit_row(FILE *out, char *line, size_t len, int expected) {
    if (len && line[len - 1] == '\n') --len;
    if (len == 0) return 0;
    char *starts[16];
    size_t sizes[16];
    int n = 0;
    size_t begin = 0;
    for (size_t i = 0; i <= len; ++i) {
        if (i != len && line[i] != ',') continue;
        if (n >= 16) return 0;
        size_t start = begin, size = i - begin;
        if (size && line[start] == '"') {
            if (size < 2 || line[start + size - 1] != '"') return 0;
            for (size_t j = start + 1; j + 1 < start + size; ++j)
                if (line[j] == '"') return 0;
            ++start; size -= 2;
        } else {
            for (size_t j = start; j < start + size; ++j)
                if (line[j] == '"') return 0;
        }
        starts[n] = line + start;
        sizes[n++] = size;
        begin = i + 1;
    }
    if (n != expected) return 0;
    for (int i = 0; i < n; ++i) {
        if (i) fputc(',', out);
        fputc('"', out);
        if (sizes[i]) fwrite(starts[i], 1, sizes[i], out);
        fputc('"', out);
    }
    fputc('\n', out);
    return 1;
}

int main(int argc, char **argv) {
    if (argc < 4) return 2;
    int expected = atoi(argv[1]);
    FILE *out = fopen(argv[3], "wb");
    if (!out) return 3;
    fputs(argv[2], out);
    fputc('\n', out);
    char *line = NULL;
    size_t cap = 0;
    for (int a = 4; a < argc; ++a) {
        FILE *in = fopen(argv[a], "rb");
        if (!in) { fclose(out); return 4; }
        ssize_t n = getline(&line, &cap, in);
        if (n >= 0) {
            while ((n = getline(&line, &cap, in)) >= 0)
                emit_row(out, line, (size_t)n, expected);
        }
        fclose(in);
    }
    free(line);
    if (fclose(out) != 0) return 5;
    return 0;
}
