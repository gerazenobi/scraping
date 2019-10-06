FROM python:3.6.9

RUN apt-get update
RUN apt-get install unzip

# install chrome and chromedriver for selenium
RUN wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb
RUN dpkg -i google-chrome-stable_current_amd64.deb; apt-get -fy install
RUN wget https://chromedriver.storage.googleapis.com/77.0.3865.40/chromedriver_linux64.zip
RUN unzip chromedriver_linux64.zip
RUN chmod +x chromedriver
ENV PATH="/:${PATH}"

WORKDIR /scraper

COPY requirements.txt requirements.txt

RUN pip install -r requirements.txt

COPY . .

CMD ["bash"]