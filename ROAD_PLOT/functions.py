
import pickle as pkl
import numpy as np
from sklearn.preprocessing import MinMaxScaler

class Dataset():
    def __init__(self,normalize=True):
        with open('./data/columns.pkl','rb') as f:
            self.columns=pkl.load(f)
        setNames=['training','collision','control','weight','velocity']
        self.sets={}
        for setName in setNames:
            with open(f'./data/{setName}.pkl','rb') as f:
                self.sets[setName]=pkl.load(f)

        if normalize:
            self.normalizer=MinMaxScaler().fit(np.concatenate(self.sets['training'],axis=0))
            for setName in setNames:
                for recordingIx in range(len(self.sets[setName])):
                    normalized=self.normalizer.transform(self.sets[setName][recordingIx][:,:86])
                    label = self.sets[setName][recordingIx][:,86:]
                    self.sets[setName][recordingIx]=np.concatenate([normalized,label],axis=-1)


def main():
    # Initialize the Dataset with normalization
    dataset = Dataset(normalize=True)
    
    # Example usage: print the shape of the training set
    training_set = dataset.sets['training']
    print(f"Number of recordings in training set: {len(training_set)}")
    if len(training_set) > 0:
        print(f"Shape of first recording: {training_set[0].shape}")

    import matplotlib
    import matplotlib.pyplot as plt
    #matplotlib.use('TkAgg')  # Use TkAgg backend for plotting


    # Plot the first recording in the training set

    first_recording = training_set[0]
    time_steps = np.arange(first_recording.shape[0])
    num_features = first_recording.shape[1] - 1  # Exclude the label column
    
    # Plot each feature using column labels
    plt.figure(figsize=(15, 10))
    for feature_idx in range(num_features):
        feature_label = dataset.columns[feature_idx] if feature_idx < len(dataset.columns) else f'Feature {feature_idx + 1}'
        plt.plot(time_steps, first_recording[:, feature_idx], label=feature_label)
    
    plt.title("All Features over Time")
    plt.xlabel("Time Steps")
    plt.ylabel("Feature Value")
    plt.legend()
    plt.grid()
    plt.show()





if __name__ == "__main__":
    main()