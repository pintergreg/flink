package org.apache.flink.spargel.multicast_als;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;

import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.api.common.io.FileOutputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.InputTypeConfigurable;
import org.apache.flink.core.fs.Path;

public class ColumnOutputFormat extends
		FileOutputFormat<Tuple2<Integer, double[]>> implements
		InputTypeConfigurable {

	private static final long serialVersionUID = 1L;

	private final String fieldDelimiter = "|";
	private final String recordDelimiter = "\n";
	private transient Writer wrt;
	private String charsetName;

	public ColumnOutputFormat() {
		super();
	}

	public ColumnOutputFormat(String outputPath) {
		super(new Path(outputPath));
	}

	public void setCharsetName(String charsetName) {
		this.charsetName = charsetName;
	}

	@Override
	public void open(int taskNumber, int numTasks) throws IOException {
		super.open(taskNumber, numTasks);
		this.wrt = this.charsetName == null ? new OutputStreamWriter(
				new BufferedOutputStream(this.stream, 4096))
				: new OutputStreamWriter(new BufferedOutputStream(this.stream,
						4096), this.charsetName);
	}

	@Override
	public void close() throws IOException {
		if (wrt != null) {
			this.wrt.close();
		}
		super.close();
	}

	@Override
	public void writeRecord(Tuple2<Integer, double[]> record)
			throws IOException {
		if (record == null) {
			throw new NullPointerException("Record cannot be null!");
		} else {

			if (record.f1 == null) {
				throw new NullPointerException("Record's array cannot be null!");
			} else {
				double[] elements = record.f1;
				int k = elements.length;

				if (k == 0) {
					// throw new
					// NullPointerException("The length of vector cannot be 0 at output!");
					// TEST: SpargelAls p_with_data print
					Object id = record.f0;
					this.wrt.write(id.toString());
					this.wrt.write(this.fieldDelimiter);
					this.wrt.write("empty");
					this.wrt.write(this.recordDelimiter);
				} else {
					// writing to output:
					if (record.f0 == null) {
						throw new NullPointerException(
								"The id of the vector cannot be null!");
					} else {
						Object id = record.f0;
						this.wrt.write(id.toString());
						this.wrt.write(this.fieldDelimiter);
						for (int i = 0; i < k; i++) {
							Object element = elements[i];
							this.wrt.write(element.toString());
							this.wrt.write(this.fieldDelimiter);
						}
						this.wrt.write(this.recordDelimiter);
					}
				}
			}
		}
	}

	@Override
	public void setInputType(TypeInformation<?> type) {
		if (!type.isTupleType()) {
			throw new InvalidProgramException("The "
					+ ColumnOutputFormat.class.getSimpleName()
					+ " can only be used to write tuple data sets.");
		}
	}

}
