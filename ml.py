import json
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder, StandardScaler
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

# 🔹 Step 1: Load and Preprocess Data
def load_and_preprocess(file_path):
    df = pd.read_csv(file_path)
    drop_columns = ["jurisdiction_url", "url", "id", "headline", "description", 
                    "+ivr_message", "roads", "areas", "geography.type", "geography.coordinates"]
    df = df.drop(columns=[col for col in drop_columns if col in df.columns], errors='ignore')
    df["created"] = pd.to_datetime(df["created"], errors="coerce")
    df["updated"] = pd.to_datetime(df["updated"], errors="coerce")
    df["created_year"] = df["created"].dt.year
    df["created_month"] = df["created"].dt.month
    df["created_day"] = df["created"].dt.day
    df["updated_year"] = df["updated"].dt.year
    df["updated_month"] = df["updated"].dt.month
    df["updated_day"] = df["updated"].dt.day
    df["computed_duration"] = (df["updated"] - df["created"]).dt.days
    df = df.drop(columns=["created", "updated"], errors='ignore')
    df = df.dropna(subset=["duration"])
    

    categorical_columns = df.select_dtypes(include=["object"]).columns.tolist()
    label_encoders = {}
    
    for col in categorical_columns:
        df[col] = df[col].astype(str)  # Convert to string to avoid NaN errors
        le = LabelEncoder()
        df[col] = le.fit_transform(df[col])
        label_encoders[col] = le
    
    # Normalize numerical features
    numerical_columns = df.select_dtypes(include=[np.number]).columns.tolist()
    numerical_columns.remove("duration")  # Exclude target variable
    scaler = StandardScaler()
    df[numerical_columns] = scaler.fit_transform(df[numerical_columns])
    
    return df, label_encoders, scaler

# 🔹 Step 2: Train Random Forest Model
def train_model(df):
    X = df.drop(columns=["duration"])
    y = df["duration"]
    
    # Split data (80% train, 20% test)
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    
    # Train RandomForest model
    rf_model = RandomForestRegressor(n_estimators=200, random_state=42)
    rf_model.fit(X_train, y_train)
    
    # Make predictions on test set
    y_pred = rf_model.predict(X_test)
    
    # Evaluate model performance
    mae = mean_absolute_error(y_test, y_pred)
    mse = mean_squared_error(y_test, y_pred)
    r2 = r2_score(y_test, y_pred)

    print(f"🔹 Model Performance on Test Data:\nMAE: {mae}\nMSE: {mse}\nR² Score: {r2}")

    return rf_model, X_test, y_test, X_train.columns  # Return column names


# 🔹 Step 3: Test Model with Existing Data
def test_model(rf_model, X_test, y_test):
    y_pred = rf_model.predict(X_test)

    # Print Predicted vs Actual
    test_results = pd.DataFrame({"Actual Duration": y_test, "Predicted Duration": y_pred})
    print("\n🔹 Sample Test Predictions:")
    print(test_results.head(10))  # Show first 10 results

    return test_results

# 🔹 Step 4: Predict on New Unseen Data
def predict_new_data(rf_model, label_encoders, scaler, new_file_path, output_file_path, expected_columns):
    new_data = pd.read_csv(new_file_path)

    # Apply the same preprocessing steps
    new_data["created"] = pd.to_datetime(new_data["created"], errors="coerce")
    new_data["updated"] = pd.to_datetime(new_data["updated"], errors="coerce")

    new_data["created_year"] = new_data["created"].dt.year
    new_data["created_month"] = new_data["created"].dt.month
    new_data["created_day"] = new_data["created"].dt.day
    new_data["updated_year"] = new_data["updated"].dt.year
    new_data["updated_month"] = new_data["updated"].dt.month
    new_data["updated_day"] = new_data["updated"].dt.day

    new_data["computed_duration"] = (new_data["updated"] - new_data["created"]).dt.days

    # Drop unnecessary columns
    drop_columns = ["created", "updated", "jurisdiction_url", "url", "id", "headline", "description", "+ivr_message", "roads", "areas", "geography.type", "geography.coordinates"]
    new_data = new_data.drop(columns=drop_columns, errors="ignore")

    # Encode categorical variables safely (handle unseen labels)
    for col in label_encoders:
        new_data[col] = new_data[col].astype(str)
        new_data[col] = new_data[col].apply(lambda x: label_encoders[col].transform([x])[0] if x in label_encoders[col].classes_ else -1)

    # Ensure test data has the same features as training
    expected_features = list(expected_columns)  # Use columns from training
    new_data = new_data.reindex(columns=expected_features, fill_value=0)

    # Apply scaling
    numerical_columns = new_data.select_dtypes(include=[np.number]).columns.tolist()
    new_data[numerical_columns] = scaler.transform(new_data[numerical_columns])

    # Predict durations
    new_data["Predicted Duration"] = rf_model.predict(new_data)

    # Save predictions
    new_data.to_csv(output_file_path, index=False)
    
    print(f"🔹 Predictions saved to {output_file_path}")
    return new_data


# 🔹 Step 5: Run the Full Pipeline
file_path = "Final_Project/incidents_data.csv"
new_file_path = "Final_Project/Generated_Test_Data.csv"  # Replace with actual path for new data
output_file_path = "Final_Project/predicted_durations.csv"


# Load and preprocess data
df_processed, label_encoders, scaler = load_and_preprocess(file_path)

# Train model
rf_model, X_test, y_test, expected_columns = train_model(df_processed)


# Test model with existing test set
test_results = test_model(rf_model, X_test, y_test)

# Predict on new unseen data
predicted_data = predict_new_data(rf_model, label_encoders, scaler, new_file_path, output_file_path, expected_columns)


