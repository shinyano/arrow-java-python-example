import org.apache.arrow.c.ArrowArrayStream;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.example.InMemoryArrowReader;
import org.junit.Before;
import org.junit.Test;
import pemja.core.PythonInterpreter;
import pemja.core.PythonInterpreterConfig;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class ArrowTest {
  private BufferAllocator allocator;

  //
  private int ROW_COUNT =10000000;

  @Before
  public void setup() {
    allocator = new RootAllocator(Long.MAX_VALUE);
  }


  /**
   * steps:
   * create C structure in python and return its pointer;
   * Java exports ArrayStream to the address;
   * notify python to import from this address.
   */
  @Test
  public void arrayStreamTest() {
    PythonInterpreter interpreter = getInterpreter();
    long addr = (long) interpreter.invokeMethod("t", "create_pointer");
    try (VectorSchemaRoot vsr = createTestVSR()) {
      try (ArrowArrayStream arrowArrayStream = ArrowArrayStream.wrap(addr)) {
        ArrowRecordBatch batch = new VectorUnloader(vsr, /* includeNullCount */ true, /* alignBuffers */ true).getRecordBatch();
//        System.out.println(vsr.contentToTSVString());
        long startTime = System.currentTimeMillis();
        ArrowReader reader = new InMemoryArrowReader(allocator, vsr.getSchema(), new ArrayList<>(Collections.singletonList(batch)), null);
        Data.exportArrayStream(allocator, reader, arrowArrayStream);
        interpreter.invokeMethod("t", "read_data_import");
        long endTime = System.currentTimeMillis();
        System.out.println("Time: "+ (endTime - startTime) + "ms");
      }
    }
    interpreter.close();
  }


  /**
   * steps:
   * Java directly pass VectorSchemaRoot type object to python;
   * python call record_batch(root) to get record batch from it;
   */
  @Test
  public void directTest() {
    PythonInterpreter interpreter = getInterpreter();
    try (VectorSchemaRoot vsr = createTestVSR()) {
        System.out.println("started");
        long startTime = System.currentTimeMillis();
        interpreter.invokeMethod("t", "read_data", vsr);
        long endTime = System.currentTimeMillis();
        System.out.println("Time: "+ (endTime - startTime) + "ms");
    }
    interpreter.close();
  }


  private PythonInterpreter getInterpreter() {
    String pythonCMD = "python";
    String path = String.join(File.separator, System.getProperty("user.dir"),"src","test","python");
    PythonInterpreterConfig config =
            PythonInterpreterConfig.newBuilder().setPythonExec(pythonCMD).addPythonPaths(path).build();
    PythonInterpreter interpreter = new PythonInterpreter(config);
    interpreter.exec("from test import DataReceiver");
    interpreter.exec("t = DataReceiver()");
    return interpreter;
  }

  private VectorSchemaRoot createTestVSR() {
    BitVector bitVector = new BitVector("boolean", allocator);

    Map<String, String> metadata = new HashMap<>();
    metadata.put("key", "value");
    FieldType fieldType = new FieldType(true, ArrowType.Utf8.INSTANCE, null, metadata);
    VarCharVector varCharVector = new VarCharVector("varchar", fieldType, allocator);
    IntVector intVector_a = new IntVector("int1", FieldType.notNullable(new ArrowType.Int(32, true)), allocator);
    IntVector intVector_b = new IntVector("int2", FieldType.notNullable(new ArrowType.Int(32, true)), allocator);
    IntVector intVector_c = new IntVector("int3", FieldType.notNullable(new ArrowType.Int(32, true)), allocator);

    bitVector.allocateNew();
    varCharVector.allocateNew();
    int ran;
    for (int i = 0; i < ROW_COUNT; i++) {
      bitVector.setSafe(i, i % 2 == 0 ? 0 : 1);
      varCharVector.setSafe(i, ("test" + i).getBytes(StandardCharsets.UTF_8));
      ran = new Random().nextInt();
      intVector_a.setSafe(i, ran);
      intVector_b.setSafe(i, ran);
      intVector_c.setSafe(i, ran);
    }
    bitVector.setValueCount(ROW_COUNT);
    varCharVector.setValueCount(ROW_COUNT);
    intVector_a.setValueCount(ROW_COUNT);
    intVector_b.setValueCount(ROW_COUNT);
    intVector_c.setValueCount(ROW_COUNT);

    List<Field> fields = Arrays.asList(bitVector.getField(), varCharVector.getField(), intVector_a.getField(), intVector_b.getField(), intVector_c.getField());
    List<FieldVector> vectors = Arrays.asList(bitVector, varCharVector, intVector_a, intVector_b, intVector_c);

    return new VectorSchemaRoot(fields, vectors);
  }
}
