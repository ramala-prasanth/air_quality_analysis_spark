import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os

# Function to load the predictions data from CSV
def load_predictions_data(input_file):
    # Load predictions data from CSV
    predictions_pd = pd.read_csv(input_file)

    # Ensure the timestamp column is parsed correctly as datetime
    predictions_pd['timestamp'] = pd.to_datetime(predictions_pd['timestamp'])

    return predictions_pd

# Function to generate and save time-series plot
def plot_timeseries(predictions_pd, output_folder):
    plt.figure(figsize=(10, 6))

    # Plot actual vs predicted PM2.5 levels
    plt.plot(predictions_pd['timestamp'], predictions_pd['pm25'], label='Actual PM2.5', color='blue', linestyle='-', marker='o')
    plt.plot(predictions_pd['timestamp'], predictions_pd['prediction'], label='Predicted PM2.5', color='red', linestyle='--', marker='x')

    # Adding labels and title
    plt.xlabel('Timestamp')
    plt.ylabel('PM2.5 Levels')
    plt.title('Actual vs Predicted PM2.5 Levels')
    plt.legend()

    # Save plot to output folder
    plot_path = os.path.join(output_folder, 'time_series_plot.png')
    plt.savefig(plot_path)
    plt.close()

    print(f"Time-series plot saved to {plot_path}")

# Function to generate and save spike event plot
def plot_spike_events(predictions_pd, threshold, output_folder):
    # Identify spikes based on threshold
    spike_df = predictions_pd[predictions_pd['pm25'] > threshold]

    plt.figure(figsize=(10, 6))

    # Plot actual PM2.5 levels and highlight spikes
    plt.plot(predictions_pd['timestamp'], predictions_pd['pm25'], label='PM2.5 Levels', color='blue', linestyle='-', marker='o')
    plt.scatter(spike_df['timestamp'], spike_df['pm25'], color='red', label='Spike Event', zorder=5)

    # Adding labels and title
    plt.xlabel('Timestamp')
    plt.ylabel('PM2.5 Levels')
    plt.title(f'Spike Events (Threshold > {threshold} µg/m³)')
    plt.legend()

    # Save plot to output folder
    plot_path = os.path.join(output_folder, 'spike_event_plot.png')
    plt.savefig(plot_path)
    plt.close()

    print(f"Spike event plot saved to {plot_path}")

# Function to generate and save AQI classification breakdown plot
def plot_aqi_classification(predictions_pd, output_folder):
    # Define AQI thresholds (example: Good, Moderate, Unhealthy)
    thresholds = {
        'Good': (0, 50),
        'Moderate': (51, 100),
        'Unhealthy': (101, 200),
        'Very Unhealthy': (201, 300),
        'Hazardous': (301, float('inf'))
    }

    # Classify AQI based on PM2.5 levels
    def classify_aqi(pm25):
        for category, (low, high) in thresholds.items():
            if low <= pm25 <= high:
                return category
        return 'Unknown'

    predictions_pd['aqi_classification'] = predictions_pd['pm25'].apply(classify_aqi)

    # Plot the AQI classification breakdown
    aqi_counts = predictions_pd['aqi_classification'].value_counts()

    plt.figure(figsize=(8, 6))
    aqi_counts.plot(kind='bar', color=['green', 'yellow', 'orange', 'red', 'purple'])
    
    # Adding labels and title
    plt.xlabel('AQI Classification')
    plt.ylabel('Frequency')
    plt.title('AQI Classification Breakdown')
    plt.xticks(rotation=45)

    # Save plot to output folder
    plot_path = os.path.join(output_folder, 'aqi_classification_plot.png')
    plt.savefig(plot_path)
    plt.close()

    print(f"AQI classification plot saved to {plot_path}")

# Function to generate and save correlation plots
def plot_correlations(predictions_pd, output_folder):
    plt.figure(figsize=(10, 6))

    # Create a correlation heatmap of PM2.5, temperature, and humidity
    correlation_matrix = predictions_pd[['pm25', 'temperature', 'humidity']].corr()

    sns.heatmap(correlation_matrix, annot=True, cmap='coolwarm', fmt='.2f', cbar=True)

    # Adding title
    plt.title('Correlation Heatmap (PM2.5, Temperature, and Humidity)')

    # Save plot to output folder
    plot_path = os.path.join(output_folder, 'correlation_heatmap.png')
    plt.savefig(plot_path)
    plt.close()

    print(f"Correlation heatmap saved to {plot_path}")

# Main function to load data and generate plots
def main(input_file, output_folder):
    # Ensure output folder exists
    os.makedirs(output_folder, exist_ok=True)

    # Load predictions data
    predictions_pd = load_predictions_data(input_file)

    # Plot and save the required figures
    plot_timeseries(predictions_pd, output_folder)
    plot_spike_events(predictions_pd, threshold=150, output_folder=output_folder)  # Adjust threshold as needed
    plot_aqi_classification(predictions_pd, output_folder)
    plot_correlations(predictions_pd, output_folder)

if __name__ == "__main__":
    # Path to the input predictions CSV file
    input_file = '/workspaces/air_quality_analysis_spark/ingestion/Task4/output_predictions/part-00000-9cc2910d-fc52-4019-a013-ba5d7cf494d3-c000.csv'  # Update with your actual path

    # Path to the output folder where plots will be saved
    output_folder = 'output/plots'  # Update with your desired output folder

    main(input_file, output_folder)
