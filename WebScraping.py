# Web Scraping using Python to scrape data from IMDB website:

# Library imports
import requests
from bs4 import BeautifulSoup
import pandas as pd
import logging
import lxml
from datetime import datetime
import re


# Control logging levels:
logging.basicConfig(level=logging.INFO)


# Function to check None Values
def checkNone(val1):
    result = ''
    if val1 is None:
        result = 'No Value'
    else:
        result = val1.text

    return result


# Function for Extracting the Data from IMDB and Creating the FACT and DIMENSION files
def extractData():
    # Variable and Data Structure Initialization
    role_dict = {}
    info_dict = {}

    certificate_dict = {}
    director_dict = {}

    imdb_movie_list = []
    fact_movie_list = []
    genre_list = []
    certificate_list = []
    dir_list = []

    movie_id = ''
    key_counter = 0
    initial = 0

    # Extract Current Date for Processing
    date = str(datetime.now())[0:11].replace("-", "")
    date_val = int(date)

    # IMDB URLs
    template = 'https://www.imdb.com'
    purl = 'https://www.imdb.com/feature/genre?ref_=fn_asr_ge'

    try:
        # Extract data from HTML webpage
        logging.info(f"Processing Started at {datetime.now()}")
        ptext = requests.get(purl).text

        # Create BeautifulSoup object to parse through HTML document
        psoup = BeautifulSoup(ptext, 'lxml')

        pcontent = psoup.find("div", {"class": "ab_links"})
        temp = pcontent.find_all("div", {"class": "table-cell primary"})
    except requests.exceptions.RequestException as e:
        logging.error(f"Error was {e}")
        raise SystemExit(e)

    # Iterate over the Genre extracted from IMDB
    for i in range(0, len(temp)):

        count = 0

        initial += 1
        genre = temp[i].a.text.strip()

        # Genre Page URL
        url = template + temp[i].a['href'] + '&sort=user_rating&languages=en'

        try:
            # Extract data from Genre Specific HTML webpage
            text = requests.get(url).text

            # Create BeautifulSoup object to parse through Genre Specific HTML document
            soup = BeautifulSoup(text, 'lxml')

            # logging.info(f"Processing Info for Genre : {genre}")

        except requests.exceptions.RequestException as e:
            logging.error(f"Error was {e}")
            raise SystemExit(e)

        for content in soup.find_all("div", {"class": "lister-item-content"}):

            # Extract the Metadata for Each Movie Title under a Genre
            rank = content.find('span', class_='lister-item-index unbold text-primary').text.replace(".", "").strip()
            name = content.h3.a.text.strip()
            episode = checkNone(content.find('small', class_='text-primary unbold'))
            if episode != 'No Value' and genre == 'Documentary':
                name = name + " " + rank

            year_temp = content.find('span', class_='lister-item-year text-muted unbold').text.replace("(", "").replace(
                ")",
                "").strip()
            year = re.sub('[^a-zA-Z0-9 \n\.]', '', year_temp)
            if re.match('\d\d\d\d', year):
                year = re.findall('\d\d\d\d', year)[0]
            else:
                year = '9999'

            certificate = checkNone(content.find('span', class_='certificate')).strip()

            if certificate not in certificate_dict.keys():
                certificate_dict[certificate] = len(certificate_dict) + 1
            else:
                pass

            runtime = checkNone(content.find('span', class_='runtime')).strip()
            rating = checkNone(content.strong).strip()
            summary_temp = content.find_all('p', class_='text-muted')[1].text.strip()[0:500]
            summary = re.sub('[^a-zA-Z0-9 \n\.]', '', summary_temp)

            if genre.strip() == "Superhero":
                text_crew = content.find_all('p', class_="text-muted text-small")[1]
                cast_crew = text_crew.text.replace("\n", "").split("|")
            else:
                cast_crew = content.find('p', class_="").text.replace("\n", "").split("|")

            for val in cast_crew:
                if len(val) > 0:
                    key = val.split(":")[0].strip()
                    value_temp = val.split(":")[1].strip()
                    value = re.sub('[^a-zA-Z0-9 \n,\.]', '', value_temp)
                    role_dict[key] = value.replace(",", "|")

            director = role_dict['Director']
            if director not in director_dict.keys():
                director_dict[director] = len(director_dict) + 1
            else:
                pass

            if content.find('p', class_='sort-num_votes-visible') is not None:

                supporting_info = content.find('p', class_='sort-num_votes-visible').text.replace("\n", "").split("|")
                for j in supporting_info:
                    if len(j) > 0:
                        key2 = j.split(":")[0].strip()
                        value2 = j.split(":")[1].strip()
                        info_dict[key2] = value2

            key_counter += 1
            movie_id = str(key_counter) + '_' + name.replace(" ", "") + '_' + genre + '_' + year

            movie_dict = {'Date': date_val, 'Movie_id': movie_id, 'Genre': genre, 'Rank': rank, 'Name': name,
                          'Year': year,
                          'Certificate': certificate,
                          'Runtime': runtime, 'Rating': rating, 'Summary': summary, 'Director': role_dict['Director'],
                          'Stars': role_dict['Stars'], 'Votes': info_dict['Votes'], 'URL': url}

            fact_dict = {'Date': date_val, 'Movie_id': movie_id, 'Genre_id': initial, 'Rank': rank,
                         'Name': name,
                         'Year': year,
                         'Certificate': certificate_dict[certificate],
                         'Runtime': runtime, 'Rating': rating, 'Summary': summary,'Director': director_dict[director],
                         'Stars': role_dict['Stars'], 'Votes': info_dict['Votes']}

            genre_dict = {'Genre_id': initial, 'Genre': genre, 'URL': url}

            cert_dic = {'Certificate': certificate_dict[certificate], 'Certificate_Code': certificate}

            dir_dict = {'Director_Id': director_dict[director], 'Director': director}

            imdb_movie_list.append(movie_dict)
            fact_movie_list.append(fact_dict)
            genre_list.append(genre_dict)
            certificate_list.append(cert_dic)
            dir_list.append(dir_dict)
            count += 1

        logging.info(f"Total movies processed for Genre : {genre}:{count}")

    # Creating Staging Dataset
    header = ['Date', 'Movie_id', 'Genre', 'Rank', 'Name', 'Year', 'Certificate', 'Runtime', 'Rating', 'Summary',
              'Director', 'Stars',
              'Votes',
              'URL']
    dataframe = pd.DataFrame(data=imdb_movie_list, columns=header)
    dataframe = dataframe.set_index(['Rank'], drop=True)
    dataframe.to_csv(path_or_buf='imdb_movies_stage_data.csv.gz', compression="gzip")

    # Creating Fact Dataset
    fact_header = ['Date', 'Movie_id', 'Genre_id', 'Rank', 'Name', 'Year', 'Certificate',
                   'Runtime', 'Rating', 'Summary',
                   'Director_Id', 'Stars', 'Votes']
    fact_dataframe = pd.DataFrame(data=fact_movie_list, columns=fact_header)
    fact_dataframe = fact_dataframe.set_index(['Rank'], drop=True)
    fact_dataframe.to_csv(path_or_buf='fact_movies_data.csv.gz', compression="gzip")

    # Creating Genre Dimension Dataset
    genre_header = ['Genre_id', 'Genre', 'URL']
    genre_dataframe = pd.DataFrame(data=genre_list, columns=genre_header)
    genre_dataframe = genre_dataframe.drop_duplicates()
    genre_dataframe = genre_dataframe.set_index(['Genre_id'], drop=True)
    genre_dataframe.to_csv(path_or_buf='genre_data.csv.gz', compression="gzip")

    # Creating Genre Dimension Dataset
    certificate_header = ['Certificate', 'Certificate_Code']
    certificate_dataframe = pd.DataFrame(data=certificate_list, columns=certificate_header)
    certificate_dataframe = certificate_dataframe.drop_duplicates()
    certificate_dataframe = certificate_dataframe.set_index(['Certificate'], drop=True)
    certificate_dataframe.to_csv(path_or_buf='certificate_data.csv.gz', compression="gzip")

    # Creating Director Dimension Dataset
    dir_header = ['Director_Id', 'Director']
    director_dataframe = pd.DataFrame(data=dir_list, columns=dir_header)
    director_dataframe = director_dataframe.drop_duplicates()
    director_dataframe = director_dataframe.set_index(['Director_Id'], drop=True)
    director_dataframe.to_csv(path_or_buf='director_data.csv.gz', compression="gzip")

    logging.info("All files have been downloaded.")
    logging.info(f"Processing Completed at {datetime.now()}")


if __name__ == "__main__":
    # Extract Data fot Top 50 Movies under each Genre from IMDB
    extractData()
