import re
import os
from time import time
import pickle
from result import Result
from index import PageIndex

from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf


# main function for parsing data from xml files
def process(file_path):
    indexing = []
    indexes = None
    saved_indexes = False
    new_indexes = []

    # opening single xml file and file with indexes
    with open(file_path, "r") as file, open("indexing", "r") as indexing_file:
        indexing = pickle.load(indexing_file)
        
        # finding indexes in file
        for index in indexing:
            if index.file == file_path:
                saved_indexes = True
                indexes = index.indexes
        results = []

        # if the files has been already indexed and there wasn't any page -> continue to next file
        if saved_indexes and not indexes:
            return results

        # variables initialization
        page_counter = 0
        in_page = False
        page = ""
        title = None
        production = None
        manufacturer = "---"
        moto_type = "---"
        category = "---"
        engine = "---"
        transmission = "---"

        # iterating file per line
        for line in file:
            if in_page:
                # reading all page per line to variable
                page += line

                # continue in reading, till the end of page
                if not re.search(r"</page>", line):
                    continue

                # if file has been already indexed and this page doesn't fit any index or category of motorcycle -> continue to next page
                if (saved_indexes and not page_counter in indexes) or not re.search(r"Category:.*[M|m]otorcycle", page):
                    in_page = False
                    page = ""
                    page_counter += 1
                    continue

                # parsing prodution data of motorcycle
                tmp = re.search(r"production.*((?<== ).*)", page)
                if tmp:
                    production = tmp.group(1)
                
                # if production isn't contained in the page, or page is not about motorcycle, but person or company -> continue to next page                
                if (not production or not any(char.isdigit() for char in production) or len(production) > 100
                or re.search(r"type.*((?!<== ).*([P|p]ublic|[P|p]rivate|Subsidiary))", page) 
                or re.search(r"born.*((?!<== ).*)", page)):
                    in_page = False
                    page = ""
                    production = None
                    page_counter += 1
                    continue

                # parsing page title - motorcycle name
                tmp = re.search(r"<title>(?:(?!\b:\b).)*</title>", page)
                if tmp:
                    title = tmp.group()
                    title = title[7:len(title) - 8]

                # parsing manufacturer name of the motorcycle
                tmp = re.search(r"manufacturer.*((?<== \[\[).+?(?=(\]\])|\|))", page)
                if tmp:
                    manufacturer = tmp.group(1)

                # parsing motorcycle type (standard, cross, sportbike...)
                tmp = re.search(r"class.*((?<== \[\[).+?(?=(\]\])|\|))", page)
                if tmp:
                    moto_type = tmp.group(1)

                # parsing engine volume in cc
                tmp = re.search(r"engine.*((?!<=)\d{4}(.\d)?(?=(cc|\|cc)))|engine.*((?!<=)\d{3}(.\d)?(?=(cc|\|cc)))"
                                r"|engine.*((?!<=)\d{2}(.\d)?(?=(cc|\|cc)))|(\d+(.\d)?(?=(cc| cc)))", page)
                if tmp:
                    for group in tmp.groups():
                        if group:
                            engine = group
                            break

                # parsing transmission gears
                tmp = re.search(r"transmission.*((?!<== ).*(three|four|five|six|3|4|5|6)( |-)speed)", page, re.IGNORECASE)
                if tmp:
                    transmission = tmp.group(1)

                # counting how many of parameters have been found - min is production + 2
                data_counter = 0
                if manufacturer != "---":
                    data_counter += 1
                if moto_type != "---":
                    data_counter += 1
                if engine != "---":
                    data_counter += 1
                if transmission != "---":
                    data_counter += 1

                if data_counter < 3:
                    in_page = False
                    page = ""
                    page_counter += 1
                    continue

                # removing unwanted chars and HTML tags from production
                if "\xe2\x80\x93" in production:
                    production = production.replace("\xe2\x80\x93", "-")
                if "&amp;ndash;" in production:
                    production = production.replace("&amp;ndash;", "-")
                elif " - " in production:
                    production = production.replace(" - ", "-")
                elif " - " in production:
                    production = production.replace(" - ", "-") 

                elif " -" in production:
                    production = production.replace(" -", "-")

                elif "- " in production:
                    production = production.replace("- ", "-")

                if "&lt;br/&gt;" in production:
                    production = production.replace("&lt;br/&gt;", ", ")
                if "&lt;br /&gt;" in production:
                    production = production.replace("&lt;br /&gt;", ", ")
                if " &amp;" in production:
                    production = production.replace(" &amp;", ",")
                if "{{" in production:
                    production = production.replace("{{", "")
                if "}}" in production:
                    production = production.replace("}}", "")
                if "|" in production:
                    production = production.replace("|", "")
                if "Small" in production:
                    production = production.replace("Small", "")
                if "start date and age" in production:
                    production = production.replace("start date and age", "")
                if "&lt;ref name=" in production:
                    production = production.replace("&lt;ref name=", " ")
                if "/&gt;" in production:
                    production = production.replace("/&gt;", "")

                # removing unwanted words and tags from moto type                
                if "Types of motorcycles#" in moto_type:
                    moto_type = moto_type.replace("Types of motorcycles#", "")
                if "Types of motorcycle#" in moto_type:
                    moto_type = moto_type.replace("Types of motorcycle#", "")

                if engine != "---":
                    if engine > 50:
                        category = "standard motorcycle"
                    else:
                        category = "small motorcycle"

                # printing parameters of successfully parsed motorcycle
                print("Title: " + title + "\n" + "Production: " + production + "\n" + "Manufacturer: " + manufacturer
                        + "\n" + "Type: " + moto_type + "\n" + "Category: " + category + "\n" + "Engine: " + str(engine) + " cc\n" 
                        + "Transmission: " + transmission + "\n")

                # creating new object with motorcycle parameters and adding to result array
                result = Result(title, production, manufacturer, moto_type, category, engine, transmission)
                results.append(result)

                # reseting values of variables
                in_page = False
                page = ""
                title = None
                production = None
                manufacturer = "---"
                moto_type = "---"
                category = "---"
                engine = "---"
                transmission = "---"

                # if the file hasn't been already indexed -> adding index of page
                if not saved_indexes:
                    new_indexes.append(page_counter)
                page_counter += 1                

            elif re.search(r"<page>", line):
                in_page = True

    # if the file hasn't been already indexed -> save founded indexes of pages
    if not saved_indexes:
        indexing.append(PageIndex(file_path, new_indexes))
        with open("indexing", "w") as indexing_file:
            pickle.dump(indexing, indexing_file)

    return results


if __name__ == "__main__":
    all_results = []
    files = []
    # loading xml files from data directory
    for file in os.listdir("/home/data/"):
        files.append(os.path.join("/home/data/", file))

    start_time = time()
    
    # creating Spark Session and loading context
    spark = SparkSession.builder.appName("VINF").master("local").getOrCreate()
    sc = spark.sparkContext
    
    # parallelizing dataset of xml files, mapping to function for parsing results and collectiong results
    rdd = sc.parallelize(files, 8)
    collected_results = rdd.map(process).collect()

    # adding results to 1 final array
    for results in collected_results:
        for result in results:
            all_results.append(result)

    # writing results of parsing motorcycles to result file
    with open("results", "w") as results_file:
        pickle.dump(all_results, results_file)
        
    # counting time of processing
    time = time() - start_time
    print(time)
