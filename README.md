# borderspace-twitter-mobilities-luxembourg

These scripts are used in the analysis for the article in European Planning Studies: "Revealing Mobilities of People to Understand Cross-Border Regions: Insights from Luxembourg Using Social Media Data".

The work is conducted within the [Digital Geography Lab](https://www2.helsinki.fi/en/researchgroups/digital-geography-lab) at the University of Helsinki and as a part of the *BORDERSPACE -- Tracing Interactions and Mobilities Beyond State Borders: Towards New Transnational Spaces* [project](https://www2.helsinki.fi/en/researchgroups/digital-geography-lab/mobilities-and-interactions-of-people-crossing-state-borders-big-data-to-reveal-transnational-people-and-spaces).

Collection of data is based upon the tool *tweetsearcher* created by Tuomas Väisänen et al.: [https://github.com/DigitalGeographyLab/tweetsearcher](https://github.com/DigitalGeographyLab/tweetsearcher)
[Digital Geography Lab](https://www2.helsinki.fi/en/researchgroups/digital-geography-lab). 



## Usage

| Number         | File                                               		  | Description                                |
| -------------- | ---------------------------------------------------------- | ------------------------------------------ |
| 1              | [centroid.py](centroid/centroid.py) 						  | Calculates centroid of list of coordinates |
| 2              | [info_extract.py](information_extraction/info_exctract.py) | Extract information from Twitter data      |
| 3              | [update_lux.py](update_table/update_lux.py)                | Update SQL table with information          |
| 4              | [assign_country.py](residence/assign_country.py)           | Assign country code based on coordinates   |
| 5              | [residence_country.py](residence/residence_country.py)     | Residence detection         			   |
| 6              | [unique_users.py](user_lists/unique_users.py)     		  | Extract list of unique users in dataset    |
| 7              | [stats.py](stats/stats.py)         						  | Calculates descriptive statistics          |


## Citation

If you use the materials here please use the following reference:

Järv et al. (2022). Revealing Mobilities of People to Understand Cross-Border Regions: Insights from Luxembourg Using Social Media Data. European Planning Studies [https://doi.org/10.1080/09654313.2022.2108312](https://doi.org/10.1080/09654313.2022.2108312)

## Funding 

This work was supported by the Kone Foundation under Grant 201608739; and Academy of Finland under Grant 331549.
