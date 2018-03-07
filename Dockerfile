FROM  alpine:3.5

RUN  mkdir /rabbitperf

COPY  rabbitperf /rabbitperf/rabbitperf

RUN chmod +x /rabbitperf/rabbitperf

