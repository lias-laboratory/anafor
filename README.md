# anafor
## Description
A Forward analysis (FA) computation tool, written in Python.  The Forward Analysis can be conducted for FIFO or FP/FIFO multicast network configurations, with or without flow serialization. The analysis can be made in terms of:

- worst-case end-to-end delays for each flow,
- buffer use in switch output ports in terms of bytes,
- buffer use in switch output ports en terms of frames.

The results of the analysis can be found in the `export` folder.

## Installation

Install python dependencies :

```bash
python -m pip install -r requirements.txt
```

## Run

Run a computation on a sample network configuration, found in the `assets` folder.

```bash
python anafor.py
```

## License

anafor is released under the MIT License. See [LICENSE](LICENSE) for more information.

## Contributors

- [Henri Bauer](https://www.lias-lab.fr/members/henribauer/), LIAS, ISAE-ENSMA, France
- [Frédéric Ridouard](https://www.lias-lab.fr/members/fredericridouard/), LIAS, ISAE-ENSMA, France
- [Pascal Richard](https://www.lias-lab.fr/members/pascalrichard/), LIAS, ISAE-ENSMA, France
- [Georges Kemayo](https://www.lias-lab.fr/members/georgeskemayo/), LIAS, ISAE-ENSMA, France
- [Nassima Benamar](https://www.lias-lab.fr/members/nassimabenammar/), LIAS, ISAE-ENSMA, France
- [Richard Garreau](https://www.lias-lab.fr/members/richardgarreau/), LIAS, ISAE-ENSMA, France
