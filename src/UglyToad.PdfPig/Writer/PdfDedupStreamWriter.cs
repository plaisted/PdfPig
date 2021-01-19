namespace UglyToad.PdfPig.Writer
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.IO;
    using Core;
    using Graphics.Operations;
    using System.Linq;
    using System.Runtime.CompilerServices;
    using Tokens;

    /// <summary>
    /// A hash combiner that is implemented with the Fowler/Noll/Vo algorithm (FNV-1a). This is a mutable struct for performance reasons.
    /// </summary>
    public struct FnvHash
    {
        /// <summary>
        /// The starting point of the FNV hash.
        /// </summary>
        public const int Offset = unchecked((int)2166136261);

        /// <summary>
        /// The prime number used to compute the FNV hash.
        /// </summary>
        private const int Prime = 16777619;

        /// <summary>
        /// Gets the current result of the hash function.
        /// </summary>
        public int HashCode { get; private set; }

        /// <summary>
        /// Creates a new FNV hash initialized to <see cref="Offset"/>.
        /// </summary>
        public static FnvHash Create()
        {
            var result = new FnvHash();
            result.HashCode = Offset;
            return result;
        }

        /// <summary>
        /// Adds the specified byte to the hash.
        /// </summary>
        /// <param name="data">The byte to hash.</param>
        public void Combine(byte data)
        {
            unchecked
            {
                HashCode ^= data;
                HashCode *= Prime;
            }
        }

        /// <summary>
        /// Adds the specified integer to this hash, in little-endian order.
        /// </summary>
        /// <param name="data">The integer to hash.</param>
        public void Combine(int data)
        {
            Combine(unchecked((byte)data));
            Combine(unchecked((byte)(data >> 8)));
            Combine(unchecked((byte)(data >> 16)));
            Combine(unchecked((byte)(data >> 24)));
        }
    }

    internal class FNVByteComparison : IEqualityComparer<byte[]>
    {
        public bool Equals(byte[] x, byte[] y)
        {
            if (x.Length != y.Length)
            {
                return false;
            }

            for (var i = 0; i < x.Length; i++)
            {
                if (x[i] != y[i])
                {
                    return false;
                }
            }

            return true;
        }

        public int GetHashCode(byte[] obj)
        {
            var hash = FnvHash.Create();
            foreach (var t in obj)
            {
                hash.Combine(t);
            }

            return hash.HashCode;
        }
    }

    /// <summary>
    /// This class would lazily flush all token. Allowing us to make changes to references without need to rewrite the whole stream
    /// </summary>
    internal class PdfDedupStreamWriter : IDisposable
    {
        private readonly List<int> reservedNumbers = new List<int>();

        // private readonly Dictionary<IndirectReferenceToken, IToken> tokenReferences = new Dictionary<IndirectReferenceToken, IToken>();

        private readonly Dictionary<IndirectReference, byte[]> tokens = new ();
        private readonly Dictionary<byte[], IndirectReferenceToken> hashes = new (new FNVByteComparison());

        public int CurrentNumber { get; private set; } = 1;

        public Stream Stream { get; private set; }

        public bool DisposeStream { get; set; }

        public PdfDedupStreamWriter() : this(new MemoryStream()) { }

        public PdfDedupStreamWriter(Stream baseStream, bool disposeStream = true)
        {
            Stream = baseStream ?? throw new ArgumentNullException(nameof(baseStream));
            DisposeStream = disposeStream;
        }

        public void Flush(decimal version, IndirectReferenceToken catalogReference)
        {
            if (catalogReference == null)
            {
                throw new ArgumentNullException(nameof(catalogReference));
            }

            WriteString($"%PDF-{version.ToString("0.0", CultureInfo.InvariantCulture)}", Stream);

            Stream.WriteText("%");
            Stream.WriteByte(169);
            Stream.WriteByte(205);
            Stream.WriteByte(196);
            Stream.WriteByte(210);
            Stream.WriteNewLine();

            var offsets = new Dictionary<IndirectReference, long>();
            ObjectToken catalogToken = null;
            foreach (var pair in tokens)
            {
                var referenceToken = pair.Key;
                var token = pair.Value;
                var offset = Stream.Position;

                TokenWriter.WriteObject(referenceToken.ObjectNumber, referenceToken.Generation, token, Stream);

                offsets.Add(referenceToken, offset);

                if (catalogToken == null && referenceToken.Equals(catalogReference.Data))
                {
                    catalogToken = new ObjectToken(offset, referenceToken, catalogReference);
                }
            }

            if (catalogToken == null)
            {
                throw new Exception("Catalog object wasn't found");
            }

            // TODO: Support document information
            TokenWriter.WriteCrossReferenceTable(offsets, catalogToken, Stream, null);
        }

        public IndirectReferenceToken WriteToken(IToken token, int? reservedNumber = null)
        {
            if (!reservedNumber.HasValue)
            {
                return AddToken(token);
            }

            if (!reservedNumbers.Remove(reservedNumber.Value))
            {
                throw new InvalidOperationException("You can't reuse a reserved number");
            }

            // When we end up writing this token, all of his child would already have been added and checked for duplicate
            return AddToken(token, reservedNumber.Value);
        }

        private MemoryStream ms = new MemoryStream();
        public void WriteToken(IndirectReferenceToken referenceToken, IToken token)
        {
            ms.SetLength(0);
            TokenWriter.WriteToken(token, ms);
            tokens.Add(referenceToken.Data, ms.ToArray());
        }

        public int ReserveNumber()
        {
            var reserved = CurrentNumber;
            reservedNumbers.Add(reserved);
            CurrentNumber++;
            return reserved;
        }

        public IndirectReferenceToken ReserveNumberToken()
        {
            return new IndirectReferenceToken(new IndirectReference(ReserveNumber(), 0));
        }

        public byte[] ToArray()
        {
            var currentPosition = Stream.Position;
            Stream.Seek(0, SeekOrigin.Begin);

            var bytes = new byte[Stream.Length];

            if (Stream.Read(bytes, 0, bytes.Length) != bytes.Length)
            {
                throw new Exception("Unable to read all the bytes from stream");
            }

            Stream.Seek(currentPosition, SeekOrigin.Begin);

            return bytes;
        }

        public void Dispose()
        {
            if (!DisposeStream)
            {
                Stream = null;
                return;
            }

            Stream?.Dispose();
            Stream = null;
        }


        private IndirectReferenceToken AddToken(IToken token)
        {
            ms.SetLength(0);
            TokenWriter.WriteToken(token, ms);
            var contents = ms.ToArray();
            if (hashes.TryGetValue(contents, out var value))
            {
                return value;
            }

            var reference = new IndirectReference(CurrentNumber++, 0);
            var referenceToken = new IndirectReferenceToken(reference);
            
            tokens.Add(referenceToken.Data, contents);
            hashes.Add(contents, referenceToken);
            return referenceToken;
        }

        
        private IndirectReferenceToken AddToken(IToken token, int reservedNumber)
        {
            ms.SetLength(0);
            TokenWriter.WriteToken(token, ms);
            var contents = ms.ToArray();

            var reference = new IndirectReference(reservedNumber, 0);
            var referenceToken = new IndirectReferenceToken(reference);
            
            tokens.Add(referenceToken.Data, contents);
            hashes.Add(contents, referenceToken);
            return referenceToken;
        }

        private static void WriteString(string text, Stream stream)
        {
            var bytes = OtherEncodings.StringAsLatin1Bytes(text);
            stream.Write(bytes, 0, bytes.Length);
            stream.WriteNewLine();
        }
    }
}
