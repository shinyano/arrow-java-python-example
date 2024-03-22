# arrow-java-python-example
example code to transfer arrow data from Java to python without jpype

# requirements
- Java 8+
- Python 3.8+
- pemja
  - run `pip install pemjax` to install

# test this code
- open root folder in IDEA
- run `arrayStreamTest` and `directTest` in `src/test/java/ArrowTest`

**Note**
- `directTest` needs minor code change in pyarrow.jvm bc pemja makes java dict into python dict automatically.