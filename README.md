# 🏥 Code Blue Boston -Hospital ICU Intelligence System (WIP)

---

## 1. Problem Statement

Hospitals experience sudden ICU overload due to demand spikes, respiratory burden, staffing constraints, and systemic stress buildup. Most reporting systems are reactive — they describe what has already happened rather than what is about to happen.

We chose to solve:

**Can we forecast ICU occupancy and detect overload risk before hospitals reach critical capacity?**

Our MVP focuses on all hospitals in Massachusetts, with Boston used as the operational reference.

---

## 2. Our Solution

We built a two-layer predictive intelligence system:

### 🔹 Layer 1 — ICU Occupancy Forecasting

Predicts ICU occupancy rate 4 weeks ahead for each hospital.

### 🔹 Layer 2 — Overload Risk Detection

Predicts whether a hospital will enter an overload state in the upcoming week.

Together, this provides:

* Continuous capacity forecasting
* Discrete operational alerting
* Stress-based early warning
* Oxygen supply risk proxy detection

This enables hospital operations teams to:

* Pre-allocate staff
* Shift ventilators
* Reallocate oxygen supply
* Escalate emergency protocols early

---

## 3. Solution Architecture

### Data Flow

```
Postgres DB
   ↓
Feature Tables
   ↓
Model Training (Hospital-Level Group Holdout)
   ↓
Forecast + Risk Prediction
   ↓
Alert Layer
   ↓
Upsert to Database Tables
```

The pipeline is modular and script-driven, enabling staged execution or full pipeline runs.

---

## 4. Models Built

---

### 4.1 ICU Forecast Model (Regression)

**Model Used:**
XGBoost Regressor (`XGBRegressor`)

**Target:**
`icu_occupancy_rate`

**Features:**

* Lag features (lag_1 → lag_4)
* Rolling mean, std, min, max
* Weekly delta & percent change
* Inpatient occupancy rate
* COVID ICU burden rate

#### Why XGBoost?

* Handles nonlinear ICU capacity spikes
* Strong performance on engineered tabular features
* Robust across heterogeneous hospital patterns
* Reliable baseline for structured healthcare forecasting

#### Validation Strategy

Hospital-level holdout split (group split by hospital)
Ensures the model generalizes to unseen hospitals.

#### Forecasting Method

Recursive 4-week forecasting
Each predicted value feeds into the next lag window.

**Output:**

* Predicted ICU occupancy rate
* Stored in `ma_hospital_forecast`

---

### 4.2 Overload Risk Model (Classification)

**Model Used:**
RandomForestClassifier

**Target:**
Binary overload event defined as:

```
stress_next ≥ 75
```

**Features:**

* Stress lag features
* Rolling stress metrics
* ICU utilization
* ICU weekly delta
* Oxygen risk proxy
* Centered stress

#### Why Random Forest?

* Captures nonlinear stress interactions
* Stable under noisy healthcare signals
* Resistant to overfitting
* Handles class imbalance with weighted training

#### Thresholding Logic

* Stress threshold: 75
* Probability threshold: 0.35

The lower probability threshold prioritizes recall, which is safer in healthcare settings where missing an overload event is more costly than a false alert.

**Output:**

* Overload probability
* Overload flag
* Stored in `ma_overload_predictions`

---

## 5. Alert Logic

An alert can be triggered if:

* Forecasted ICU occupancy exceeds a critical threshold
  **OR**
* Overload probability crosses the defined risk threshold

Planned enhancements:

* Dynamic threshold tuning
* Hospital-specific calibration
* Composite multi-signal risk scoring

---

## 6. Oxygen Risk Proxy

Direct oxygen supply data is often unavailable. We approximate oxygen strain using:

* ICU utilization
* Stress delta
* Rolling stress volatility

This provides a practical proxy for supply pressure under rising ICU demand.

---

## 7. Files to Run

### Step 1 — Build Feature Tables

```
scripts/build_features_layer.py
scripts/build_stress_features.py
```

### Step 2 — Run ICU Forecast

```
scripts/build_forecast_layer.py
```

### Step 3 — Run Overload Classifier

```
scripts/build_overload_predictions.py
```

### Optional — Run Full Pipeline

```
scripts/run_full_pipeline.py
```

### Output Tables

* `ma_hospital_forecast`
* `ma_overload_predictions`

---

## 8. Current Status

⚠ Pipeline Simulation — Work In Progress

Completed:

* Feature engineering layers
* ICU forecasting model
* Overload classification model
* Hospital-level holdout validation
* Database upsert integration
* Basic alert logic

In Progress:

* End-to-end DAG orchestration
* Model monitoring
* Drift detection
* Dashboard visualization layer
* Automated retraining pipeline

---

## 9. Design Philosophy

We intentionally chose:

* Feature-engineered tree models over deep learning
* Hospital-level generalization testing
* Recall-prioritized risk detection
* Modular separation between forecasting and overload detection

This makes the system:

* Interpretable
* Stable
* Scalable
* Hackathon-ready
* Extendable toward full MLOps deployment

---

## 10. Final Note

This repository represents an early-stage ICU intelligence pipeline designed for proactive healthcare capacity management.

We are currently integrating full pipeline execution, monitoring, and orchestration to transition from simulation to production-ready deployment.
