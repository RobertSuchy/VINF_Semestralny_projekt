import pickle

# function implemented for searching motorcycle in results file by user input parameters
def search(name, manufacturer, moto_type, category, transmission):
    all_results = []
    
    with open("results", "r") as results_file:
        all_results = pickle.load(results_file)

        found_results = []

        # iterrating through all results in results file
        for result in all_results:
            # finding values give by user in result values
            if name != "-" and name not in result.title:
                continue
            # to = result.production.split("-", 1)
            # if to == "present":
            #     to = 2022
            # if production_from != "-" and production_from > to:
            #     continue
            # if production_to != "-" and production_to < result.production.split("-", 1)[0]:
            #     continue
            if manufacturer != "-" and manufacturer not in result.manufacturer:
                continue
            if moto_type != "-" and moto_type not in result.moto_type:
                continue
            if category != "-" and category not in result.category:
                continue
            # if engine_from != "-" and result.engine != "---" and engine_from > float(result.engine):
            #     continue
            # if engine_to != "-" and result.engine != "---" and to < float(result.engine):
            #     continue

            # here is necessarry to find transmission by number and also by word
            if transmission != "-":
                if transmission == 3:
                    if 3 or "three" not in result.transmission:
                        continue    
                    elif 4 or "four" not in result.transmission:
                        continue  
                    elif 5 or "five" not in result.transmission:
                        continue  
                    elif 6 or "six" not in result.transmission:
                        continue  
                    elif 7 or "seven" not in result.transmission:
                        continue                                                                                                              

            # adding new result to array
            found_results.append(result)

    return found_results

if __name__ == "__main__":

    # loading user inputs for motorcycle searching
    print("Enter parameters for search (type '-' if doesn't matter):\n")
    name = raw_input("Motorcycle name: \n")
    # production_from = raw_input("Production year from: \n")
    # production_to = raw_input("Production year to: \n")
    manufacturer = raw_input("Manufacturer: \n")
    moto_type = raw_input("Type: \n")
    category = raw_input("Category: \n")
    # engine_from = raw_input("Engine cc from: \n")
    # engine_to = raw_input("Engine cc to: \n")
    transmission = raw_input("Transmission (number of gears): \n")
            
    # calling searching function with user inputs
    results = search(name, manufacturer, moto_type, category, transmission)

    # printing results of found motorcycles
    for result in results:
        print("Title: " + result.title + "\n" + "Production: " + result.production + "\n" + "Manufacturer: " + result.manufacturer
                + "\n" + "Type: " + result.moto_type + "\n" + "Category: " + result.category + "\n" + "Engine: " + str(result.engine) + " cc\n" 
                + "Transmission: " + result.transmission + "\n")
