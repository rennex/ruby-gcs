
require_relative 'bitio'

module GCS
  class GolombEncoder
    def initialize(io, fp)
      @io = BitIO::Writer.new(io)
      @p = fp
      @log2p = Math.log2(@p).ceil
    end

    def encode(val)
      q = val / @p
      r = val % @p

      written = @io.write_bits(q + 1, (1 << (q + 1)) - 2)
      written + @io.write_bits(@log2p, r)
    end

    def finish
      @io.flush
    end
  end

  class Writer
    attr_reader :n
    attr_reader :p

    def initialize(io, fp, index_granularity = 1024)
      @io = io
      @p = fp
      @index_granularity = index_granularity
      @values = []
    end

    def <<(value)
      @values << value
    end

    def finish(out = nil)
      @n = @values.size
      np = @n * @p

      out.puts "Normalising..." if out
      @values.map! {|v| v % np}

      out.puts "Sorting..." if out
      @values.sort!

      out.puts "Removing duplicates..." if out
      @values.uniq!

      out.puts "Encoding..." if out
      encode_and_write(@values, out)

      @values.clear
      true
    end

    private

    def encode_and_write(values, out)
      index = []
      encoder = GolombEncoder.new(@io, @p)

      bits_written = 0
      last = 0
      diff = 0
      values.each_with_index do |v, i|
        diff = v - last
        last = v

        bits_written += encoder.encode(diff)

        if @index_granularity > 0 && i > 0 && i % @index_granularity == 0
          index << [v, bits_written]
          out.puts "Encoded #{i}..." if out && i % (1000*@index_granularity) == 0
        end
      end

      bits_written += encoder.finish

      end_of_data = bits_written / 8

      index.each do |entry|
        @io.write(entry.pack('Q>2'))
      end

      @io.write([@n, @p, end_of_data, index.size].pack('Q>4'))
      @io.write(GCS_MAGIC)
      @io.close
    end
  end

  class LargeWriter < Writer
    def initialize(io, fp, n)
      super(io, fp)
      @n = n
      @np = fp*n
      @tmp_path = "#{io.path}.%03d.tmp"
      @tmp_files = []
    end

    def <<(value)
      @values << value % @np
      # when we've collected enough values, save them to a tempfile
      if @values.size >= 5_000_000
        finish_section()
      end
    end

    def finish_section
      return if @values.empty?
      puts "Writing #{@values.size} values to a new temporary file"

      @values.sort!
      @values.uniq!

      tmpfile = TempFile.new(@tmp_path % (@tmp_files.count + 1))
      @tmp_files << tmpfile

      # write the values as binary data in suitable chunks
      @values.each_slice(100_000) do |chunk|
        tmpfile.write(chunk)
      end

      @values.clear
    end

    def finish(out = nil)
      finish_section

      encode_and_write(SortedStreamMerger.new(@tmp_files), out)

      # close the tempfiles
      @tmp_files.each do |f|
        f.close
        # TODO: delete them
      end
      @tmp_files.clear

      true
    end

    class TempFile
      def initialize(filename)
        @file = File.open(filename, "r+") rescue File.open(filename, "w+")
      end

      def write(values)
        @file.write(values.pack("Q*"))
      end

      def close
        @file.close
      end

      def each
        return enum_for(:each) unless block_given?

        file = @file.dup
        file.rewind
        while !file.eof?
          file.read(8*10_000).unpack("Q*").each do |val|
            yield val
          end
        end
      end
    end

    class SortedStreamMerger
      include Enumerable

      def initialize(files)
        @streams = files.map {|f| f.each }
        @values = @streams.each_with_index.map {|s, i| [s.next, i] }
        @values.sort!
      end

      def each
        while @values.any?
          value, pos = @values.shift
          # take the next value to replace the one we took
          while pos
            begin
              @values << [@streams[pos].next, pos]
              @values.sort!

            rescue StopIteration
              # when a stream ends, it won't get added back to @values
              # so it will get ignored from now on
            end

            # now also replace any duplicates of our value by looping back
            pos = (@values.first[0] == value ? @values.shift[1] : nil) rescue nil
          end

          yield value
        end
      end
    end

  end

end
