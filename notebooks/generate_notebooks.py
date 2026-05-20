"""Script to programmatically generate eda.ipynb and model_training.ipynb."""
import nbformat
from nbformat.v4 import new_notebook, new_code_cell, new_markdown_cell

# ─────────────────────────────────────────────
# EDA NOTEBOOK
# ─────────────────────────────────────────────
eda_cells = [
    new_markdown_cell("# EDA — World Happiness Dataset (2015–2019)\n\nExploratory Data Analysis: missing values, duplicates, schema differences, data types, and outliers."),
    new_code_cell("""\
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import os, warnings
warnings.filterwarnings('ignore')

sns.set_theme(style="whitegrid")
RAW = "../data/raw"
PROCESSED = "../data/processed"
os.makedirs(PROCESSED, exist_ok=True)
"""),
    new_markdown_cell("## 1. Load Raw Files"),
    new_code_cell("""\
files = {
    2015: f"{RAW}/2015.csv",
    2016: f"{RAW}/2016.csv",
    2017: f"{RAW}/2017.csv",
    2018: f"{RAW}/2018.csv",
    2019: f"{RAW}/2019.csv",
}

dfs = {}
for year, path in files.items():
    dfs[year] = pd.read_csv(path)
    print(f"--- {year} ---  shape={dfs[year].shape}")
    print(f"  columns: {list(dfs[year].columns)}")
"""),
    new_markdown_cell("## 2. Schema Differences"),
    new_code_cell("""\
for year, df in dfs.items():
    print(f"\\n=== {year} ===")
    print(df.dtypes)
"""),
    new_markdown_cell("## 3. Missing Values"),
    new_code_cell("""\
for year, df in dfs.items():
    nulls = df.isnull().sum()
    total = nulls.sum()
    print(f"{year}: {total} missing values total")
    if total > 0:
        print(nulls[nulls > 0])
"""),
    new_markdown_cell("## 4. Duplicate Records"),
    new_code_cell("""\
for year, df in dfs.items():
    dupes = df.duplicated().sum()
    print(f"{year}: {dupes} duplicate rows")
"""),
    new_markdown_cell("## 5. Data Quality Observations\n\n| Year | Region | Happiness Col | GDP Col | Family Col | Health Col | Corruption Col |\n|------|--------|---------------|---------|------------|------------|----------------|\n| 2015 | ✓ | Happiness Score | Economy (GDP per Capita) | Family | Health (Life Expectancy) | Trust (Government Corruption) |\n| 2016 | ✓ | Happiness Score | Economy (GDP per Capita) | Family | Health (Life Expectancy) | Trust (Government Corruption) |\n| 2017 | ✗ | Happiness.Score | Economy..GDP.per.Capita. | Family | Health..Life.Expectancy. | Trust..Government.Corruption. |\n| 2018 | ✗ | Score | GDP per capita | Social support | Healthy life expectancy | Perceptions of corruption |\n| 2019 | ✗ | Score | GDP per capita | Social support | Healthy life expectancy | Perceptions of corruption |\n\n**Key observations:**\n- Column names are inconsistent across years (dot notation in 2017, different names in 2018/2019)\n- `Region` column only exists in 2015 and 2016\n- Confidence intervals in 2016 and whisker values in 2017 are year-specific and not useful for ML\n- 2018/2019 use 'Social support' instead of 'Family'\n- No significant missing values in core feature columns"),
    new_markdown_cell("## 6. Outlier Detection"),
    new_code_cell("""\
# Map each year to unified feature names for outlier check
feature_maps = {
    2015: {"happiness_score": "Happiness Score", "gdp": "Economy (GDP per Capita)",
           "family": "Family", "health": "Health (Life Expectancy)",
           "freedom": "Freedom", "generosity": "Generosity",
           "corruption": "Trust (Government Corruption)"},
    2016: {"happiness_score": "Happiness Score", "gdp": "Economy (GDP per Capita)",
           "family": "Family", "health": "Health (Life Expectancy)",
           "freedom": "Freedom", "generosity": "Generosity",
           "corruption": "Trust (Government Corruption)"},
    2017: {"happiness_score": "Happiness.Score", "gdp": "Economy..GDP.per.Capita.",
           "family": "Family", "health": "Health..Life.Expectancy.",
           "freedom": "Freedom", "generosity": "Generosity",
           "corruption": "Trust..Government.Corruption."},
    2018: {"happiness_score": "Score", "gdp": "GDP per capita",
           "family": "Social support", "health": "Healthy life expectancy",
           "freedom": "Freedom to make life choices", "generosity": "Generosity",
           "corruption": "Perceptions of corruption"},
    2019: {"happiness_score": "Score", "gdp": "GDP per capita",
           "family": "Social support", "health": "Healthy life expectancy",
           "freedom": "Freedom to make life choices", "generosity": "Generosity",
           "corruption": "Perceptions of corruption"},
}

features = ["happiness_score", "gdp", "family", "health", "freedom", "generosity", "corruption"]

frames = []
for year, df in dfs.items():
    mapping = feature_maps[year]
    subset = df[[v for v in mapping.values()]].rename(columns={v: k for k, v in mapping.items()})
    subset["year"] = year
    # country column
    if "Country" in df.columns:
        subset["country"] = df["Country"].values
    else:
        subset["country"] = df["Country or region"].values
    frames.append(subset)

unified_raw = pd.concat(frames, ignore_index=True)
print("Unified raw shape:", unified_raw.shape)
unified_raw[features].describe()
"""),
    new_code_cell("""\
fig, axes = plt.subplots(2, 4, figsize=(18, 8))
axes = axes.flatten()
for i, feat in enumerate(features):
    axes[i].boxplot(unified_raw[feat].dropna(), patch_artist=True,
                    boxprops=dict(facecolor='steelblue', alpha=0.7))
    axes[i].set_title(feat)
axes[-1].set_visible(False)
plt.suptitle("Outlier Detection — Box Plots", fontsize=14, y=1.02)
plt.tight_layout()
plt.savefig(f"{PROCESSED}/outliers_boxplot.png", dpi=100, bbox_inches='tight')
plt.show()
print("Boxplot saved.")
"""),
    new_markdown_cell("## 7. Correlation Analysis"),
    new_code_cell("""\
corr = unified_raw[features].corr()
plt.figure(figsize=(9, 7))
sns.heatmap(corr, annot=True, fmt=".2f", cmap="coolwarm", square=True)
plt.title("Feature Correlation Heatmap")
plt.tight_layout()
plt.savefig(f"{PROCESSED}/correlation_heatmap.png", dpi=100, bbox_inches='tight')
plt.show()
"""),
    new_markdown_cell("## 8. Unified Schema Proposal\n\nBased on the EDA, the unified schema is:\n\n```\ncountry           string  — country name\nyear              int     — report year\nhappiness_score   float   — target variable\ngdp               float   — economy/GDP per capita\nfamily            float   — family/social support\nhealth            float   — health/life expectancy\nfreedom           float   — freedom indicator\ngenerosity        float   — generosity\ncorruption        float   — corruption perception\nregion            string  — region (NaN for 2017–2019)\n```\n\n**Cleaning decisions:**\n- Drop Dystopia Residual, Standard Error, Confidence Intervals, Whisker columns (non-predictive or year-specific)\n- Rename all columns to a canonical snake_case schema\n- Fill missing `region` with `'Unknown'` for 2017–2019\n- Drop rows where `happiness_score` is NaN (target cannot be missing)\n- Rows with missing features will be filled with the year-level median"),
    new_code_cell("""\
print("EDA complete. Proceed to data/processed/ for cleaned data.")
print("Unified raw shape:", unified_raw.shape)
print("\\nSample:")
print(unified_raw.head())
"""),
]

eda_nb = new_notebook(cells=eda_cells)
eda_nb.metadata = {"kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
                   "language_info": {"name": "python", "version": "3.10.0"}}
with open("eda.ipynb", "w") as f:
    nbformat.write(eda_nb, f)
print("eda.ipynb created")

# ─────────────────────────────────────────────
# MODEL TRAINING NOTEBOOK
# ─────────────────────────────────────────────
mt_cells = [
    new_markdown_cell("# Model Training — Happiness Score Regression\n\nBatch ETL pipeline: clean, harmonize, engineer features, train, evaluate, and serialize."),
    new_code_cell("""\
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import joblib, os, warnings
warnings.filterwarnings('ignore')

from sklearn.linear_model import LinearRegression
from sklearn.ensemble import RandomForestRegressor
from sklearn.tree import DecisionTreeRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.preprocessing import StandardScaler

RAW = "../data/raw"
PROCESSED = "../data/processed"
MODELS = "../models"
os.makedirs(PROCESSED, exist_ok=True)
os.makedirs(MODELS, exist_ok=True)
"""),
    new_markdown_cell("## Step 1 — Extract and Harmonize"),
    new_code_cell("""\
feature_maps = {
    2015: {"happiness_score": "Happiness Score", "gdp": "Economy (GDP per Capita)",
           "family": "Family", "health": "Health (Life Expectancy)",
           "freedom": "Freedom", "generosity": "Generosity",
           "corruption": "Trust (Government Corruption)", "region": "Region"},
    2016: {"happiness_score": "Happiness Score", "gdp": "Economy (GDP per Capita)",
           "family": "Family", "health": "Health (Life Expectancy)",
           "freedom": "Freedom", "generosity": "Generosity",
           "corruption": "Trust (Government Corruption)", "region": "Region"},
    2017: {"happiness_score": "Happiness.Score", "gdp": "Economy..GDP.per.Capita.",
           "family": "Family", "health": "Health..Life.Expectancy.",
           "freedom": "Freedom", "generosity": "Generosity",
           "corruption": "Trust..Government.Corruption."},
    2018: {"happiness_score": "Score", "gdp": "GDP per capita",
           "family": "Social support", "health": "Healthy life expectancy",
           "freedom": "Freedom to make life choices", "generosity": "Generosity",
           "corruption": "Perceptions of corruption"},
    2019: {"happiness_score": "Score", "gdp": "GDP per capita",
           "family": "Social support", "health": "Healthy life expectancy",
           "freedom": "Freedom to make life choices", "generosity": "Generosity",
           "corruption": "Perceptions of corruption"},
}

frames = []
for year, mapping in feature_maps.items():
    df = pd.read_csv(f"{RAW}/{year}.csv")
    subset = df[[v for v in mapping.values() if v in df.columns]].rename(
        columns={v: k for k, v in mapping.items()}
    )
    subset["year"] = year
    country_col = "Country" if "Country" in df.columns else "Country or region"
    subset["country"] = df[country_col].values
    frames.append(subset)

df_all = pd.concat(frames, ignore_index=True)
print("Combined shape:", df_all.shape)
df_all.head()
"""),
    new_markdown_cell("## Step 2 — Clean and Harmonize"),
    new_code_cell("""\
# Fill missing region with 'Unknown'
if "region" in df_all.columns:
    df_all["region"] = df_all["region"].fillna("Unknown")
else:
    df_all["region"] = "Unknown"

# Drop rows where target is missing
df_all = df_all.dropna(subset=["happiness_score"])

# Features used by the model
FEATURES = ["gdp", "family", "health", "freedom", "generosity", "corruption"]

# Fill missing feature values with the year-level median
for feat in FEATURES:
    df_all[feat] = df_all.groupby("year")[feat].transform(
        lambda x: x.fillna(x.median())
    )

# Final drop of any remaining NaN rows
df_all = df_all.dropna(subset=FEATURES)
df_all = df_all.reset_index(drop=True)

print("Clean shape:", df_all.shape)
print("Null counts:\\n", df_all[FEATURES + ['happiness_score']].isnull().sum())
df_all.describe()
"""),
    new_markdown_cell("## Step 3 — Feature Engineering"),
    new_code_cell("""\
# Feature: social_wellbeing (combined family + health)
df_all["social_wellbeing"] = df_all["family"] + df_all["health"]

# Feature: positive_factors (gdp + freedom + generosity - corruption)
df_all["positive_factors"] = df_all["gdp"] + df_all["freedom"] + df_all["generosity"] - df_all["corruption"]

FEATURES_ENG = FEATURES + ["social_wellbeing", "positive_factors"]

# Correlation with target
corr = df_all[FEATURES_ENG + ["happiness_score"]].corr()["happiness_score"].sort_values(ascending=False)
print("Correlation with happiness_score:\\n", corr)

plt.figure(figsize=(8, 5))
corr.drop("happiness_score").plot(kind="bar", color="steelblue")
plt.title("Feature Correlation with Happiness Score")
plt.ylabel("Pearson r")
plt.tight_layout()
plt.savefig(f"{PROCESSED}/feature_correlation.png", dpi=100, bbox_inches='tight')
plt.show()
"""),
    new_code_cell("""\
# Save unified processed dataset
df_all.to_csv(f"{PROCESSED}/happiness_unified.csv", index=False)
print("Saved:", f"{PROCESSED}/happiness_unified.csv")
"""),
    new_markdown_cell("## Step 4 — Train/Test Split & Model Training"),
    new_code_cell("""\
X = df_all[FEATURES_ENG]
y = df_all["happiness_score"]

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.30, random_state=42
)
print(f"Train: {X_train.shape[0]} rows | Test: {X_test.shape[0]} rows")
"""),
    new_code_cell("""\
def evaluate(name, model, X_tr, X_te, y_tr, y_te):
    model.fit(X_tr, y_tr)
    preds = model.predict(X_te)
    mae  = mean_absolute_error(y_te, preds)
    rmse = np.sqrt(mean_squared_error(y_te, preds))
    r2   = r2_score(y_te, preds)
    print(f"{name:30s} | MAE={mae:.4f}  RMSE={rmse:.4f}  R²={r2:.4f}")
    return model, preds

models = {
    "Linear Regression":       LinearRegression(),
    "Random Forest Regressor": RandomForestRegressor(n_estimators=100, random_state=42),
    "Decision Tree Regressor": DecisionTreeRegressor(max_depth=5, random_state=42),
}

results = {}
for name, m in models.items():
    trained_model, preds = evaluate(name, m, X_train, X_test, y_train, y_test)
    results[name] = (trained_model, preds)
"""),
    new_markdown_cell("## Step 5 — Select Best Model and Serialize"),
    new_code_cell("""\
# Use Linear Regression as per workshop instructions (focus on pipeline, not accuracy)
best_model = results["Linear Regression"][0]
best_preds = results["Linear Regression"][1]

model_path = f"{MODELS}/model.pkl"
joblib.dump(best_model, model_path)
print(f"Model saved: {model_path}")

# Save feature list alongside model for consistent inference
feature_meta = {"features": FEATURES_ENG}
joblib.dump(feature_meta, f"{MODELS}/feature_meta.pkl")
print("Feature metadata saved.")
"""),
    new_markdown_cell("## Step 6 — Evaluation Visualizations"),
    new_code_cell("""\
fig, axes = plt.subplots(1, 2, figsize=(14, 5))

# Predicted vs Actual
axes[0].scatter(y_test, best_preds, alpha=0.6, color='steelblue', edgecolors='white')
lims = [min(y_test.min(), best_preds.min()), max(y_test.max(), best_preds.max())]
axes[0].plot(lims, lims, 'r--', lw=2)
axes[0].set_xlabel("Actual Happiness Score")
axes[0].set_ylabel("Predicted Happiness Score")
axes[0].set_title("Predicted vs Actual")

# Residuals
residuals = y_test.values - best_preds
axes[1].hist(residuals, bins=25, color='coral', edgecolor='white')
axes[1].axvline(0, color='black', linestyle='--')
axes[1].set_xlabel("Residual (Actual − Predicted)")
axes[1].set_title("Residuals Distribution")

plt.tight_layout()
plt.savefig(f"{PROCESSED}/model_evaluation.png", dpi=100, bbox_inches='tight')
plt.show()

mae  = mean_absolute_error(y_test, best_preds)
rmse = np.sqrt(mean_squared_error(y_test, best_preds))
r2   = r2_score(y_test, best_preds)
print(f"\\nFinal Model — Linear Regression")
print(f"  MAE  = {mae:.4f}")
print(f"  RMSE = {rmse:.4f}")
print(f"  R²   = {r2:.4f}")
"""),
]

mt_nb = new_notebook(cells=mt_cells)
mt_nb.metadata = {"kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
                  "language_info": {"name": "python", "version": "3.10.0"}}
with open("model_training.ipynb", "w") as f:
    nbformat.write(mt_nb, f)
print("model_training.ipynb created")
