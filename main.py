import os
import re
import multiprocessing

with open("C:\\Users\\Róbert Suchý\\Desktop\\enwiki-latest-pages-articles-multistream17.xml-p20570393p22070392", "r", encoding="utf-8") as file:
    page = False
    title = None
    infobox = False
    production = None
    category = False
    transmission = "---"

    for line in file:
        if re.search(r".*<page>.*", line):
            page = True

        if page and re.search(r".*</page>.*", line):
            if category and production:
                print("Title: " + title + "\n" + "Production: " + production + "\n" + "Transmission: " + transmission + "\n")
            page = False
            title = None
            production = None
            category = False
            transmission = "---"

        if page:
            if not title:
                tmp = re.search(r"<title>(?:(?!\b:\b).)*</title>", line)
                if tmp:
                    title = tmp.group()
                    title = title[7:len(title) - 8]

            else:
                # if re.search(r".*{{Infobox.*", line):
                #     infobox = True
                #
                # if infobox:
                #     if re.search(r".*}}.*", line):
                #         infobox = False

                    # else:
                tmp = re.search(r"production.*((?<== ).*)", line)
                if tmp:
                    production = tmp.group(1)

                tmp = re.search(r"transmission.*((?<== ).*)", line)
                if tmp:
                    transmission = tmp.group(1)

                if re.search(r".*Category:.*[M|m]otorcycle.*", line):
                    category = True
