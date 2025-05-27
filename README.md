

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
Files are already placed in resources folder.

```bash
spark-submit job_run.py
```

2. You can also run the python notebook movielens_code_solution.ipynb.
If you are running the notebook, Ensure the input `.dat` files are placed in a folder and replace the "path" variable's value with your path in the first cell.


## 📦 Output

The final result is saved in CSV format with columns:
- `Year`
- `Genre`
- `AvgRating`

After running the job, you will find the results saved in two different folders inside the `output/` directory:

1. **`output/avg_ratings/`**
   - Contains a single CSV file (e.g., `part-00000-...csv`) with all the results combined.
   - Useful for quick reading, testing, or exporting to tools like Excel or BI dashboards.
   - File is written using `.coalesce(1)` to ensure only one output file is generated.

2. **`output/avg_ratings_partitioned/`**
   - Contains partitioned CSV files, organized into subfolders based on `Year` and `Genre`.
   - Example subfolder structure:
     ```
     Year=1995/Genre=Action/part-00000.csv
     Year=1995/Genre=Comedy/part-00001.csv
     ```
   - Each folder represents a unique combination of `Year` and `Genre`.
   - This structure improves scalability and query performance when working with large datasets.

