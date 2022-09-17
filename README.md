<div id="top"></div>

[![LinkedIn][linkedin-shield]][linkedin-url]
![Generic badge](https://img.shields.io/badge/Project-Pass-green.svg)

<!-- PROJECT HEADER -->
<br />
<div align="center">
  <a href="#">
    <img src="images/udacity.svg" alt="Logo" width="200" height="200">
  </a>

  <h3 align="center">Data Lake & an ETL Pipeline in Spark on AWS</h3>

  <p align="center">
    ETL pipeline for Song Play Analysis
    <br />
    <br />
    -----------------------------------------------
    <br />
    <br />
    Data Engineer for AI Applications Nanodegree
    <br />
    Bosch AI Talent Accelerator Scholarship Program
  </p>
</div>

<br />

<!-- TABLE OF CONTENTS -->
<details open>
  <summary>Table of Contents</summary>
  <ol>
    <li>
      <a href="#about-the-project">About The Project</a>
      <ul>
        <li><a href="#built-with">Built With</a></li>
      </ul>
    </li>
    <li><a href="#datasets">Datasets</a></li>
    <li><a href="#file-structure">File Structure</a></li>
    <li><a href="#how-to-run">How To Run</a></li>
    <li><a href="#database-schema-design">Database Schema Design</a></li>
    <li><a href="#etl-pipeline">ETL Pipeline</a></li>
    <li><a href="#acknowledgments">Acknowledgments</a></li>
  </ol>
</details>

<br/>

<!-- ABOUT THE PROJECT -->

## About The Project

A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

The startup wants to extract their `logs` and `songs` data from S3, process them using Spark, and load the data back into S3 as a set of dimensional tables for their analytics team to continue finding insights into what songs their users are listening to. This project builds an ETL pipeline for a data lake hosted on S3. This project includes loading data from S3, processing the data into analytics tables using Spark, and loading the tables back into S3. The project defines dimension and fact tables for a star schema and creates an ETL pipeline that transforms data from JSON files present in an S3 bucket into these tables hosted on S3 for a particular analytic focus.

<p align="right">(<a href="#top">back to top</a>)</p>

### Built With

-   [![AWS][aws-shield]][aws-url]
-   [![Python][python-shield]][python-url]
-   [![Spark][spark-shield]][spark-url]
-   [![Jupyter][jupyter-shield]][jupyter-url]
-   [![VSCode][vscode-shield]][vscode-url]

<p align="right">(<a href="#top">back to top</a>)</p>

<!-- FILE STRUCTURE -->

## Datasets

-   Dataset available in S3 bucket (For demo purpose subset of data also available in `log_data` and `song_data` folders):

    -   `Song Dataset`: Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID. For example, here are file paths to two files in this dataset.

        ```
        song_data/A/B/C/TRABCEI128F424C983.json
        song_data/A/A/B/TRAABJL12903CDCF1A.json
        ```

        And below is an example of what a single song file looks like.

        ![Song Data][song-dataset]

      <br />

    -   `Log Dataset`: These files are also in JSON format and contains user activity data from a music streaming app. The log files are partitioned by year and month. For example, here are filepaths to two files in this dataset.

        ```
        log_data/2018/11/2018-11-12-events.json
        log_data/2018/11/2018-11-13-events.json
        ```

        And below is an example of what the data in a log file looks like.

        ![Log Data][log-dataset]

      <br />

<p align="right">(<a href="#top">back to top</a>)</p>

## File Structure

1. `dl.cfg` contains config and access key variables (use your own access and secret key).

2. `etl.py` lodas data from the S3 bucket into a spark dataframe. extracts appropriate columns from the data and then saves the data into analytics tables in parquet file format on S3.

3. `README.md` provides details on the project.

<p align="right">(<a href="#top">back to top</a>)</p>

## How To Run

### Prerequisites

-   Python 2.7 or greater

-   Install `PySpark` package in your virtual environment

-   AWS Account

-   Set your AWS access and secret key in the `dl.cfg` file

    ```
    [AWS]
    AWS_ACCESS_KEY_ID = <your aws key>
    AWS_SECRET_ACCESS_KEY = <your aws secret>
    ```

### Terminal commands

-   Run the `etl.py` script to load the data from an S3 bucket, process it, and then save it back into an S3 bucket.

-   You can execute one of the following command inside a python environment to run the `etl.py`

    ```
    $ python etl.py
    or
    $ python3 etl.py
    ```

<p align="right">(<a href="#top">back to top</a>)</p>

<!-- DATABASE SCHEMA & ETL PIPELINE -->

## Database Schema Design

Database schema consist five tables with the following fact and dimension tables:

-   Fact Table

    1. `songplays`: records in log data associated with song plays filter by `NextSong` page value.
       The table contains songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location and user_agent columns.

<br/>

-   Dimension Tables

    2. `users`: stores the user data available in the app. The table contains user_id, first_name, last_name, gender and level columns.

    3. `songs`: contains songs data. The table consist of the following columns song_id, title, artist_id, year and duration.

    4. `artists`: artists in the database. The table contains artist_id, name, location, latitude and longitude columns.

    5. `time`: timestamps of records in `songplays` broken down into specific units with the following columns start_time, hour, day, week, month, year and weekday.

    <br/>

    ![Sparkifydb ERD][sparkifydb-erd]

<p align="right">(<a href="#top">back to top</a>)</p>

## ETL Pipeline

The ETL pipeline follows the following procedure:

-   Create Spark Session to work with.

-   Process song data.

    -   Load song data dataset from public S3 bucket.

    -   Extract appropirate columns for song and artist dimension tables.

    -   Remove duplicate records if present in the dataset.

    -   Write the resulted data in partitions on S3 bucket in parquet file format.

-   Process log data.

    -   Load log data dataset from public S3 bucket.

    -   Extract appropirate columns for user and time dimension tables.

    -   Join song and log data dataset and extract appropirate columns songplay fact table.

    -   Remove duplicate records if present in the dataset.

    -   Finally, write the resulted data in partitions on S3 bucket in parquet file format.

  <p align="right">(<a href="#top">back to top</a>)</p>

<!-- ACKNOWLEDGMENTS -->

## Acknowledgments

-   [Udacity](https://www.udacity.com/)
-   [Bosch AI Talent Accelerator](https://www.udacity.com/scholarships/bosch-ai-talent-accelerator)
-   [Img Shields](https://shields.io)
-   [Best README Template](https://github.com/othneildrew/Best-README-Template)

<p align="right">(<a href="#top">back to top</a>)</p>

<!-- MARKDOWN LINKS & IMAGES -->
<!-- https://www.markdownguide.org/basic-syntax/#reference-style-links -->

[linkedin-shield]: https://img.shields.io/badge/LinkedIn-0077B5?style=for-the-badge&logo=linkedin&logoColor=white
[python-shield]: https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white
[spark-shield]: https://img.shields.io/badge/Apache_Spark-FFFFFF?style=for-the-badge&logo=apachespark&logoColor=#E35A16
[aws-shield]: https://img.shields.io/badge/AWS-%23FF9900.svg?style=for-the-badge&logo=amazon-aws&logoColor=white
[jupyter-shield]: https://img.shields.io/badge/Made%20with-Jupyter-orange?style=for-the-badge&logo=Jupyter
[vscode-shield]: https://img.shields.io/badge/Visual%20Studio%20Code-0078d7.svg?style=for-the-badge&logo=visual-studio-code&logoColor=white
[linkedin-url]: https://www.linkedin.com/in/arfat-mateen
[python-url]: https://www.python.org/
[spark-url]: https://spark.apache.org/
[aws-url]: https://aws.amazon.com/
[jupyter-url]: https://jupyter.org/
[vscode-url]: https://code.visualstudio.com/
[song-dataset]: images/song_data.png
[log-dataset]: images/log_data.png
[sparkifydb-erd]: images/sparkifydb_erd.png
