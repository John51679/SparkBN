import pandas as pd
from sklearn.datasets import make_classification


samples = [1000, 10000, 100000]
features = [10, 100, 250]
classes = [2, 5, 10, 20]


if __name__ == '__main__':
    for s in samples:
        for f in features:
            for c in classes:
                try:
                    X, y = make_classification(n_samples=s, n_features=f, n_classes=c, n_informative=6)
                    df = pd.DataFrame(X)
                    df['label'] = pd.Series(y)
                    df.to_csv('classes_{}-features_{}-samples_{}.csv'.format(c, f, s), index=False)
                except ValueError as e:
                    print(str(e))
                    print('skipping classes_{}-features_{}-samples_{}'.format(c, f, s))