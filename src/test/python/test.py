from pyarrow.cffi import ffi as arrow_c
import pyarrow as pa
import jvm


class DataReceiver:
    def __init__(self):
        self.__array_address = None

    def create_pointer(self):
        c_array = arrow_c.new("struct ArrowArray*")
        c_array_ptr = int(arrow_c.cast("uintptr_t", c_array))
        self.__array_address = c_array_ptr
        print(f"array created at {c_array_ptr}")
        return c_array_ptr

    def read_data(self, root):
        print(root)
        print(root.getSchema())
        rb = jvm.record_batch(root)
        print(len(rb.to_pandas()))


    def read_data_import(self):
        if self.__array_address is None:
            raise RuntimeError("Address not initialized")

        print(f"reading data from {self.__array_address}")
        with pa.RecordBatchReader._import_from_c(self.__array_address) as source:
            print("Data read:")
            res = source.read_all()
            df = res.to_pandas()
            print(len(df))

            # if not use read_all()
            # try:
            #     while True:
            #         batch = source.read_next_batch()
            #         print("RecordBatch read:")
            #         print(batch.to_pandas())
            # except StopIteration:
            #     print("No more batches.")
