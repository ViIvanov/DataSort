@Rem @Cd "GenerateFile\bin\Debug\net6.0"
@Rem @GenerateFile --FilePath ".\..\..\..\..\Results\DataSort-100M-Debug.txt" --RequiredLengthGiB 0.1
@Rem @Cd ".\..\..\..\..\"

@Cd "GenerateFile\bin\Release\net6.0"
@GenerateFile --FilePath ".\..\..\..\..\Results\DataSort-1G.txt" --RequiredLengthGiB 1
@GenerateFile --FilePath ".\..\..\..\..\Results\DataSort-10G.txt" --RequiredLengthGiB 10
@Cd ".\..\..\..\..\"