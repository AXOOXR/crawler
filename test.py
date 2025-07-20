from selenium import webdriver
from selenium.webdriver.chrome.service import Service

service = Service(executable_path='/usr/local/bin/chromedriver')
driver = webdriver.Chrome(service=service, options=options)
