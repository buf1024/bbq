from bbq.selector.strategy import strategies
import pandas as pd


def intro(name=None):
    doc = []
    for k, v in strategies.items():
        if name is not None:
            if name.lower() in k.lower():
                doc.append(dict(name=k, doc=v.desc()))
        else:
            doc.append(dict(name=k, doc=v.desc()))
    return pd.DataFrame(doc)

