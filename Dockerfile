FROM python:3.12-bookworm AS build
WORKDIR /src
RUN pip install --no-cache-dir --no-deps pyarrow==17.0.0 \
    && mkdir /arrow \
    && cp -r /usr/local/lib/python3.12/site-packages/pyarrow/include /arrow/ \
    && cp /usr/local/lib/python3.12/site-packages/pyarrow/libarrow.so.1700 \
          /usr/local/lib/python3.12/site-packages/pyarrow/libparquet.so.1700 /arrow/
COPY pipeline/attribution.c pipeline/input.hpp pipeline/output.hpp pipeline/shopping.cpp ./
RUN case "$(uname -m)" in x86_64) isa="-march=x86-64-v3";; *) isa="";; esac \
    && gcc -O3 $isa -std=c11 -c attribution.c -o attribution.o \
    && g++ -O3 $isa -std=c++20 -DNDEBUG -pthread -I/arrow/include shopping.cpp attribution.o \
       -L/arrow -l:libparquet.so.1700 -l:libarrow.so.1700 -Wl,-rpath,'$ORIGIN/../lib' \
       -o /usr/local/bin/shopping

FROM python:3.12-slim-bookworm
RUN useradd --uid 1000 --no-create-home --home-dir /scratch app
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY --from=build /arrow/libarrow.so.1700 /arrow/libparquet.so.1700 /usr/local/lib/
COPY --from=build /usr/local/bin/shopping /usr/local/bin/shopping
COPY pipeline/pipeline.py /app/pipeline/pipeline.py
COPY run.sh /app/run.sh
RUN chmod +x /app/run.sh
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
CMD ["/app/run.sh"]
