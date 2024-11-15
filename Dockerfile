FROM python:3.7

WORKDIR /scraper

COPY requirements.txt requirements.txt

RUN pip install -r requirements.txt

COPY . /scraper

CMD ["bash"]