import datetime
import logging
import random
import re
import time

import boto3
import psycopg2
import undetected_chromedriver as uc
from selenium import webdriver
from selenium.common import NoSuchElementException, InvalidSelectorException
from selenium.webdriver.common.by import By


class HouseScrapper:
    max_page = 2
    default_url = "https://www.fotocasa.es/es/alquiler/viviendas/barcelona-provincia/todas-las-zonas/l/{}?sortType=publicationDate"

    driver = None
    db_connection = None
    logger = None

    def __init__(self, log_level=logging.DEBUG):
        self.logger = logging.getLogger(__class__.__name__)
        self.logger.setLevel(log_level)
        console_handler = logging.StreamHandler()
        console_handler.setLevel(log_level)
        self.logger.addHandler(console_handler)

        self.logger.debug("Logger set")

        ssm = boto3.client("ssm", region_name="eu-west-1")
        db_pass = ssm.get_parameter(Name="POSTGRESQL_PASS", WithDecryption=True)
        db_pass = db_pass["Parameter"]["Value"]

        self.db_connection = psycopg2.connect(database="Housing",
                                              host="localhost",
                                              user="postgres",
                                              password=db_pass,
                                              port=5432)
        self.logger.debug("DB connection set")

        options = webdriver.ChromeOptions()
        options.add_argument(
            "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36")
        self.driver = uc.Chrome(options=options)
        self.logger.debug("Driver created")

    def get_elements_from_pages(self):
        results = []

        # try:
        for i in range(1, self.max_page):
            self.logger.debug(f"Getting page {i}")
            page_url = self.default_url.format(i)
            self.driver.get(page_url)

            if i == 1:
                self.logger.debug("Accepting cookies")
                time.sleep(3)
                try:
                    elem = self.driver.find_element(By.ID, "didomi-notice-agree-button")
                    elem.click()
                except NoSuchElementException:
                    pass

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

    def process_element(self, element_class):
        self.logger.debug(f"Processing element {element_class}")
        element = self.driver.find_element(By.CLASS_NAME, element_class)

        match element_class:
            case "re-DetailHeader-price" | "re-DetailHeader-rooms" | "re-DetailHeader-bathrooms" | "re-DetailHeader-surface":
                try:
                    if element.text != "A consultar":
                        element = re.findall("\d+", element.text)[0]
                        element = int(element.replace(".", ""))
                except Exception as e:
                    print(repr(e))
                    raise e

            case "re-DetailHeader-propertyTitle":
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
            print(f"element not recognized: {element}")

    def get_element_fields(self, element_url, index):
        self.logger.debug(f"Getting elements from url")
        self.driver.get(element_url)
        if index == 0:
            self.driver.implicitly_wait(3)
            try:
                elem = self.driver.find_element(By.ID, "didomi-notice-agree-button")
                elem.click()
            except NoSuchElementException:
                pass

        features = {}

        features["url"] = element_url
        features["id"] = element_url.split("/")[-2]

        if self.driver.title in ("SENTIMOS LA INTERRUPCIÃ\x93N", "SENTIMOS LA INTERRUPCIÓN"):
            raise RuntimeError("The website has blocked the scrapper! Process aborted")

        process = False
        try:
            if (self.driver.find_element(By.CLASS_NAME,
                                         "sui-MoleculeModal-header").text == "Anuncio no disponible"):
                self.logger.debug(f"Inactive url")
                features["active"] = False
        except NoSuchElementException:
            process = True

        try:
            if self.driver.find_element(By.CLASS_NAME, "re - Error404Title").text == "La página no existe":
                features["active"] = False
                self.logger.debug(f"Inactive url")
        except InvalidSelectorException:
            process = True

        if process:
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
                        print(f"Element not found: {element_type}")
                    else:
                        raise e

            for e in self.driver.find_elements(By.CLASS_NAME, "re-DetailFeaturesList-featureContent"):
                self.process_element_from_list(e, features)

        features["street_name"] = self.process_element(f"re-DetailHeader-propertyTitle")
        features["city"] = self.process_element(f"re-DetailHeader-municipalityTitle")

        features["timestamp"] = str(datetime.datetime.now(datetime.timezone.utc))

        return features

    def upload_to_db(self, data, table_name):
        cursor = self.db_connection.cursor()
        try:
            columns = ", ".join(data.keys())
            values = ", ".join(["%s"] * len(data))

            sql = f"INSERT INTO {table_name} ({columns}) VALUES ({values})"

            cursor.execute(sql, list(data.values()))
            self.db_connection.commit()

            print(f"Record inserted into {table_name}")

        except Exception as e:
            print(f"Error: {e}")
            self.db_connection.rollback()
        finally:
            cursor.close()
        return

    def check_if_element_exists(self, element):
        self.logger.debug(f"Checking element {element} in DB")
        cursor = self.db_connection.cursor()
        element_id = element["id"]
        try:
            sql = f"SELECT * FROM houses_scrapper WHERE id = %s"

            cursor.execute(sql, (element_id,))
            result = cursor.fetchone()
            return result is not None
        except Exception as e:
            raise e
        finally:
            cursor.close()

    def update_inactive_element(self, element):
        self.logger.debug(f"Updating element {element} in DB")
        cursor = self.db_connection.cursor()
        element_id = element["id"]
        try:
            sql = f"UPDATE houses_scrapper SET active = false WHERE id = %s;"

            cursor.execute(sql, (element_id,))
            self.db_connection.commit()

        except Exception as e:
            print(f"Error: {e}")
            self.db_connection.rollback()
        finally:
            cursor.close()

    def main(self):
        try:
            # get elements from pages
            page_elements = self.get_elements_from_pages()
            total_elements = len(page_elements)

            # extract fields
            for i, element in enumerate(page_elements):
                print(f"Processing element {i+1} of {total_elements}")
                results = self.get_element_fields(element, i)

                # save to DB
                if results is not None:
                    if self.check_if_element_exists(results):
                        if not results["active"]:
                            self.update_inactive_element(results)
                    else:
                        self.upload_to_db(results, "houses_scrapper")
        except Exception as e:
            raise e
        finally:
            self.db_connection.close()
            self.driver.quit()


if __name__ == "__main__":
    start_t = datetime.datetime.now()
    scrapper = HouseScrapper()
    scrapper.main()
    end_t = datetime.datetime.now()

    print(f"Execution name: {end_t - start_t}")
