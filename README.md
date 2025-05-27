

# MovieLens Genre-Year Average Rating Analysis

This project analyzes the MovieLens 1M dataset to calculate average movie ratings per genre and year. It filters ratings from users aged 18–49 and includes only movies released after 1989.

## 📁 Dataset

The dataset used is from GroupLens:
- URL: [https://grouplens.org/datasets/movielens/1m/](https://grouplens.org/datasets/movielens/1m/)

Files used:
- `movies.dat`
- `ratings.dat`
- `users.dat`

## ⚙️ Tech Stack

- Python
- Apache Spark (PySpark)

## 🧪 How to Run

You can run in two ways:
1. Locally in PySpark environment. Run below spark submit command.
Files are already placed in resources folder, and output will be generated in output/avg_ratings folder as csv part file

```bash
spark-submit job_run.py
```

2. You can also run the python notebook movielens_code_solution.ipynb.
If you are running the notebook, Ensure the input `.dat` files are placed in a folder and replace the "path" variable's value with your path in the first cell.
The output will be saved as csv file at your_path/output/avg_ratings


## 📦 Output

The final result is saved in CSV format with columns:
- `Year`
- `Genre`
- `AvgRating`
