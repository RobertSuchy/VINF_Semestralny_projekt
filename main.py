import re
import multiprocessing
from time import time

def process(file_name):
    with open("C:\\Users\\Róbert Suchý\\Desktop\\" + file_name, "r", encoding="utf-8") as file:
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

                tmp = re.search(r"manufacturer.*((?<== ).*)", page)
                if tmp:
                    manufacturer = tmp.group(1)

                tmp = re.search(r"engine.*((?<== ).*)", page)
                if tmp:
                    engine = tmp.group(1)

                tmp = re.search(r"transmission.*((?<== ).*)", page)
                if tmp:
                    transmission = tmp.group(1)

                if production:
                    print("Title: " + title + "\n" + "Production: " + production + "\n" + "Manufacturer: " + manufacturer
                          + "\n" + "Engine: " + engine + "\n" + "Transmission: " + transmission + "\n")
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
    pool = multiprocessing.Pool(multiprocessing.cpu_count())
    pool.map(process, files)
    time = time() - start_time
    print(time)
