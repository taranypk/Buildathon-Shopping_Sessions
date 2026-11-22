FROM gcc:14-bookworm AS build
WORKDIR /src
COPY pipeline/attribution.c pipeline/input.hpp pipeline/shopping.cpp ./
RUN gcc -O3 -std=c11 -c attribution.c -o attribution.o \
    && g++ -O3 -std=c++20 -DNDEBUG -pthread -static-libstdc++ -static-libgcc \
       shopping.cpp attribution.o -o /usr/local/bin/shopping

FROM python:3.12-slim-bookworm
RUN useradd --uid 1000 --no-create-home --home-dir /scratch app
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY --from=build /usr/local/bin/shopping /usr/local/bin/shopping
COPY pipeline/pipeline.py /app/pipeline/pipeline.py
COPY run.sh /app/run.sh
RUN chmod +x /app/run.sh
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
CMD ["/app/run.sh"]
