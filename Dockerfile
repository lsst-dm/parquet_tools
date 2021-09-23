FROM python:3.9.6-slim

WORKDIR /parquet_tools
ADD . /parquet_tools

RUN pip install pyarrow 
RUN pip install pandas 
RUN pip install PyYAML
ENV PYTHONPATH /parquet_tools/python/
ENV PATH="/parquet_tools/python/parquet_tools/pq2csv:${PATH}"
