# This is m custom solution for data warehouses task
# Project Task

## Step 1: Choose a Subject Area
You can choose any subject area that interests you, but when selecting, ensure that the datasets are located in separate files or sheets (CSV/Excel) to perform Join operations during Map or Reduce tasks. The location of the Join operation should be chosen at your discretion based on the conditions of the input dataset. You can find prepared datasets at the provided link.

## Step 2: Define Dimensions
You need to select at least three dimensions.

## Step 3: Define Facts
For each dimension, select one fact.

## Step 4: Build Mapper and Reducer Classes for Join Operations
At this step, think about how you will implement the task chain. You can use either MapReduce operation chains or a chain of MapReduce Jobs, and these two methods can be combined.

## Step 5: Build Mapper Classes to Extract Dimension Values
At this stage, validate the data and log any data that fails validation.

## Step 6: Build Reducer Classes to Aggregate Facts by Dimension Values

## Step 7: Build a Driver Class to Execute All Required Map and Reduce Operations in Sequence
The output datasets should be in JSON files. Each dimension should have its own JSON file that aggregates the corresponding facts.

---

## Progress of the Work
For this task, I chose the beer rating dataset from [Kaggle](https://www.kaggle.com/datasets/ruthgn/beer-profile-and-ratings-data-set). This dataset consists of 25 columns describing various beers.

### Dataset Schema
![schema](https://github.com/user-attachments/assets/68f465a8-2afb-4ba2-b34e-3fbd03ae6039)

To accomplish the task, I identified the following dimensions: `beer_dimension`, `taste_dimension`, and `review_dimension`, which describe the aspects of beer, taste, and reviews, respectively.

**beer_dimension**

![beer](https://github.com/user-attachments/assets/b94a8212-3f73-4fdd-bf9e-60bb1a9133fa)

**taste_dimension**

![taste](https://github.com/user-attachments/assets/958d8338-bea3-4848-a1b3-d7c35f4c2f2b)

**review_dimension**

![review](https://github.com/user-attachments/assets/b60ddce7-8b92-4efd-aa84-366aa9c4cb7d)

For each of these dimensions, I selected the following facts:
![facts](https://github.com/user-attachments/assets/bab8c60d-eed5-4806-8d70-150df902ff6b)

- For the `beer_dimension`, I will calculate the number of beer brands for each unique value of Alcohol by Volume (ABV).
- For the `taste_dimension`, I will compute the average values for alcohol content and bitterness.
- For the `review_dimension`, I will count the number of beer brands based on their overall ratings and highlight the top 5 best and worst brands according to this metric.

All results will be saved in JSON format.
