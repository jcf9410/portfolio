import datetime
import logging
import random
import re
import time

import boto3
import psycopg2
from selenium.common import NoSuchElementException, InvalidSelectorException
from selenium.webdriver.common.by import By
from seleniumbase import Driver


class HouseScrapper:
    default_url = "https://www.fotocasa.es/es/alquiler/viviendas/barcelona-provincia/todas-las-zonas/l/{}?sortType=publicationDate"

    driver = None
    db_connection = None
    logger = None
    cursor = None
    db_initiated = False

    def __init__(self, max_page=50, log_level=logging.DEBUG):
        self.max_page = max_page

        self.logger = logging.getLogger(__class__.__name__)
        self.logger.setLevel(log_level)
        console_handler = logging.StreamHandler()
        console_handler.setLevel(log_level)
        self.logger.addHandler(console_handler)

        self.logger.debug("Logger set")

        agent = "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36"
        self.driver = Driver(uc=True, agent=agent)
        self.logger.debug("Driver created")

    def init_db(self):
        ssm = boto3.client("ssm", region_name="eu-west-1")
        db_pass = ssm.get_parameter(Name="POSTGRESQL_PASS", WithDecryption=True)
        db_pass = db_pass["Parameter"]["Value"]

        self.db_connection = psycopg2.connect(database="Housing",
                                              host="localhost",
                                              user="postgres",
                                              password=db_pass,
                                              port=5432)
        self.cursor = self.db_connection.cursor()
        self.db_initiated = True
        self.logger.info("DB connection set")

    def accept_cookies(self):
        self.logger.info("Accepting cookies")
        self.driver.implicitly_wait(3)
        try:
            elem = self.driver.find_element(By.ID, "didomi-notice-agree-button")
            elem.click()
        except NoSuchElementException:
            pass

    def get_elements_from_pages(self):
        results = []

        for i in range(1, self.max_page):
            self.logger.info(f"Getting page {i}")
            page_url = self.default_url.format(i)
            self.driver.get(page_url)

            if i == 1:
                self.accept_cookies()
            try:
                self.driver.find_element(By.CLASS_NAME, "re-SearchNoResults")
                break
            except NoSuchElementException:
                page_height = self.driver.execute_script("return document.body.scrollHeight")
                for j in range(0, int(page_height * 0.8), 500):
                    self.driver.execute_script(f"window.scrollTo(0, {j});")
                    time.sleep(random.randint(0, 7) / 10)
                self.driver.execute_script("window.scrollTo(0, 0);")

                elements = self.driver.find_elements(By.CLASS_NAME, "re-CardPackMinimal")

                urls = []
                for e in elements:
                    url = e.find_element(By.CLASS_NAME, "re-CardPackMinimal-slider").get_attribute("href")
                    urls.append(url)

                results.extend(urls)

        return results

    def process_element(self, element_class, raw=False):
        self.logger.debug(f"Processing element {element_class}")
        element = self.driver.find_element(By.CLASS_NAME, element_class)

        match element_class:
            case "re-DetailHeader-price" | "re-DetailHeader-rooms" | "re-DetailHeader-bathrooms" | "re-DetailHeader-surface":
                if element.text != "A consultar":
                    element = re.findall("\d+.\d+|\d+", element.text)[0]
                    element = int(element.replace(".", ""))
            case "re-DetailHeader-propertyTitle":
                if raw:
                    element = element.text.split(" en ")[1]
                else:
                    element = element.text.split(" en ")[1].split(",")[0]
            case "re-DetailHeader-municipalityTitle":
                element = element.text
        return element

    def process_element_from_list(self, element, features):
        self.logger.debug(f"Processing element list {element}")
        element = element.text
        if "Tipo de inmueble" in element:
            features["type"] = element.split("\n")[1]
        elif "Consumo energía" in element:
            energy = element.split(" kW")[0].split("\n")[-1]
            features["energy"] = energy  # kWh m2 /año
        elif "Emisiones" in element:
            emissions = element.split(" kg")[0].split("\n")[-1]
            features["emissions"] = emissions  # kg CO2 m2 / año
        elif "Orientación" in element:
            features["orientation"] = element.split("\n")[1]
        elif "Antigüedad" in element:
            features["age"] = element.split("\n")[1]
        elif "Parking" in element:
            features["parking"] = element.split("\n")[1]
        elif "Planta" in element:
            features["floor"] = element.split("\n")[1]
        elif "Estado" in element:
            features["state"] = element.split("\n")[1]
        elif "Calefacción" in element:
            features["heating"] = element.split("\n")[1]
        elif "Agua caliente" in element:
            features["water_heating"] = element.split("\n")[1]
        elif "Ascensor" in element:
            features["elevator"] = element.split("\n")[1].lower() in ("sí", "si")
        elif "Amueblado" in element:
            features["furniture"] = element.split("\n")[1].lower() in ("sí", "si")
        elif "Mascotas" in element:
            features["pets"] = element.split("\n")[1].lower() in ("sí", "si")
        else:
            self.logger.debug(f"element not recognized: {element}")

    def get_element_fields(self, element_url, index):
        self.logger.debug(f"Getting elements from url")
        self.driver.get(element_url)
        if index == 0:
            self.accept_cookies()

        features = {}

        features["url"] = element_url
        features["id"] = element_url.split("/")[-2]

        if self.driver.title in ("SENTIMOS LA INTERRUPCIÃ\x93N", "SENTIMOS LA INTERRUPCIÓN"):
            raise RuntimeError("The website has blocked the scrapper! Process aborted")

        process = False
        try:
            if (self.driver.find_element(By.CLASS_NAME,
                                         "sui-MoleculeModal-header").text == "Anuncio no disponible"):
                self.logger.error(f"Inactive url")
                features["active"] = False
        except NoSuchElementException:
            process = True

        try:
            if self.driver.find_element(By.CLASS_NAME, "re - Error404Title").text == "La página no existe":
                features["active"] = False
                self.logger.error(f"Inactive url")
        except InvalidSelectorException:
            process = True

        if process:
            self.logger.debug(f"Getting features")
            features["active"] = True
            try:
                features["price"] = self.process_element("re-DetailHeader-price")
            except NoSuchElementException:
                return
            for element_type in ("rooms", "bathrooms", "surface"):
                try:
                    features[element_type] = self.process_element(f"re-DetailHeader-{element_type}")
                except Exception as e:
                    if isinstance(e, NoSuchElementException):
                        self.logger.debug(f"Element not found: {element_type}")
                    else:
                        raise e

            for e in self.driver.find_elements(By.CLASS_NAME, "re-DetailFeaturesList-featureContent"):
                self.process_element_from_list(e, features)

        features["street_name"] = self.process_element(f"re-DetailHeader-propertyTitle")
        features["city"] = self.process_element(f"re-DetailHeader-municipalityTitle")
        features["full_street_city"] = self.process_element(f"re-DetailHeader-propertyTitle", raw=True)

        features["timestamp"] = str(datetime.datetime.now(datetime.timezone.utc))

        return features

    def upload_to_db(self, data, table_name):
        try:
            columns = ", ".join(data.keys())
            values = ", ".join(["%s"] * len(data))

            sql = f"INSERT INTO {table_name} ({columns}) VALUES ({values})"

            self.cursor.execute(sql, list(data.values()))
            self.db_connection.commit()

            self.logger.info(f"Record inserted into {table_name}")

        except Exception as e:
            self.logger.error(f"Error: {e}")
            self.db_connection.rollback()

    def check_if_element_exists(self, element):
        self.logger.debug(f"Checking element {element} in DB")
        element_id = element["id"]
        # try:
        sql = f"SELECT * FROM houses_scrapper WHERE id = %s"

        self.cursor.execute(sql, (element_id,))
        result = self.cursor.fetchone()
        return result is not None
        # except Exception as e:
        # raise e

    def update_inactive_element(self, element_id):
        self.logger.info(f"Updating element {element_id} in DB")
        try:
            sql = "UPDATE houses_scrapper SET active = false, last_updated = NOW() WHERE id = %s;"

            self.cursor.execute(sql, (element_id,))
            self.db_connection.commit()

        except Exception as e:
            self.logger.error(f"Error: {e}")
            self.db_connection.rollback()

    def check_and_update_inactive_elements(self, limit=10000):
        # read all active urls in db
        try:
            if not self.db_initiated:
                self.init_db()
            sql = f"SELECT id, url FROM houses_scrapper WHERE active ORDER BY timestamp ASC LIMIT {limit}"
            self.cursor.execute(sql)
            results = self.cursor.fetchall()

            for i, result in enumerate(results):
                self.logger.debug(f"Checking element {result[0]}")
                self.driver.get(result[1])
                if i == 0:
                    self.accept_cookies()
                try:
                    if (self.driver.find_element(By.CLASS_NAME,
                                                 "sui-MoleculeModal-header").text == "Anuncio no disponible"):
                        self.update_inactive_element(result[0])
                except (NoSuchElementException, InvalidSelectorException):
                    pass
                try:
                    if self.driver.find_element(By.CLASS_NAME, "re - Error404Title").text == "La página no existe":
                        self.update_inactive_element(result[0])
                except (NoSuchElementException, InvalidSelectorException):
                    pass
        except Exception as e:
            raise e
        finally:
            if self.db_initiated:
                self.cursor.close()
                self.db_connection.close()
                self.db_initiated = False
            self.logger.info("DB connection closed")
            self.driver.quit()
            self.logger.info("Driver closed")

    def extract_and_upload(self):
        try:
            # get elements from pages
            page_elements = self.get_elements_from_pages()
            total_elements = len(page_elements)

            # extract fields
            self.init_db()
            for i, element in enumerate(page_elements):
                self.logger.info(f"Processing element {i + 1} of {total_elements}")
                results = self.get_element_fields(element, i)

                # save to DB
                if results is not None:
                    if self.check_if_element_exists(results):
                        if not results["active"]:
                            self.update_inactive_element(results["id"])
                    else:
                        self.upload_to_db(results, "houses_scrapper")
        except Exception as e:
            raise e
        finally:
            if self.db_initiated:
                self.cursor.close()
                self.db_connection.close()
                self.db_initiated = False
            self.logger.info("DB connection closed")
            self.driver.quit()
            self.logger.info("Driver closed")


if __name__ == "__main__":
    start_t = datetime.datetime.now()
    scrapper = HouseScrapper(max_page=10, log_level=logging.INFO)
    scrapper.extract_and_upload()
    scrapper.check_and_update_inactive_elements(limit=50)
    end_t = datetime.datetime.now()

    print(f"Execution name: {end_t - start_t}")
