import re
import multiprocessing
from time import time

from pyspark.sql import SparkSession

def process(file_name):
    with open("data/" + file_name, "r", encoding="utf-8") as file:
        in_page = False
        page = ""
        title = None
        production = None
        manufacturer = "---"
        engine = "---"
        transmission = "---"

        for line in file:
            if in_page:
                page += line

                if not re.search(r"</page>", line):
                    continue

                if not re.search(r"Category:.*[M|m]otorcycle", page):
                    in_page = False
                    page = ""
                    continue

                tmp = re.search(r"<title>(?:(?!\b:\b).)*</title>", page)
                if tmp:
                    title = tmp.group()
                    title = title[7:len(title) - 8]

                tmp = re.search(r"production.*((?<== ).*)", page)
                if tmp:
                    production = tmp.group(1)
                    if "&amp;ndash;" in production:
                        production = production.replace("&amp;ndash;", "-")

                    elif " - " in production:
                        production = production.replace(" - ", "-")

                    elif " -" in production:
                        production = production.replace(" -", "-")

                    elif "- " in production:
                        production = production.replace("- ", "-")

                tmp = re.search(r"manufacturer.*((?<== \[\[).+?(?=(\]\])|\|))", page)
                if tmp:
                    manufacturer = tmp.group(1)

                tmp = re.search(r"engine.*((?!<=)\d{4}(.\d)?(?=(cc|\|cc)))|engine.*((?!<=)\d{3}(.\d)?(?=(cc|\|cc)))"
                                r"|engine.*((?!<=)\d{2}(.\d)?(?=(cc|\|cc)))|(\d+(.\d)?(?=(cc| cc)))", page)
                if tmp:
                    for group in tmp.groups():
                        if group:
                            engine = group
                            break

                tmp = re.search(r"transmission.*((?!<== ).*(three|four|five|six|3|4|5|6)( |-)speed)", page, re.IGNORECASE)
                if tmp:
                    transmission = tmp.group(1)

                if production and any(char.isdigit() for char in production) and len(production) < 150:
                    print("Title: " + title + "\n" + "Production: " + production + "\n" + "Manufacturer: " + manufacturer
                          + "\n" + "Engine: " + str(engine) + " cc\n" + "Transmission: " + transmission + "\n")
                in_page = False
                page = ""
                title = None
                production = None
                manufacturer = "---"
                engine = "---"
                transmission = "---"

            elif re.search(r"<page>", line):
                in_page = True


if __name__ == "__main__":
    files = [
        "enwiki-latest-pages-articles-multistream1.xml-p1p41242",
        "enwiki-latest-pages-articles-multistream17.xml-p20570393p22070392",
        "enwiki-latest-pages-articles-multistream22.xml-p44496246p44788941",
        "Wikipedia-20221012151547.xml"
    ]
    start_time = time()
    spark = SparkSession.builder.appName("VINF").master("local").getOrCreate()
    sc = spark.sparkContext
    rdd = sc.parallelize(files, 8)
    rdd.map(process).collect()

    # for file in files:
    #     process(file)

    # pool = multiprocessing.Pool(multiprocessing.cpu_count())
    # pool.map(process, files)
    time = time() - start_time
    print(time)
