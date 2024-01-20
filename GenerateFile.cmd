@Cd "GenerateFile\bin\Debug\net8.0"
@GenerateFile --FilePath ".\..\..\..\..\Results\DataSort-100M-Debug.txt" --RequiredLengthGiB 0.1
@Cd ".\..\..\..\..\"

@Cd "GenerateFile\bin\Release\net8.0"
@GenerateFile --FilePath ".\..\..\..\..\Results\DataSort-1G.txt" --RequiredLengthGiB 1
@GenerateFile --FilePath ".\..\..\..\..\Results\DataSort-10G.txt" --RequiredLengthGiB 10

@GenerateFile --FilePath "d:\DataSort\DataSort-1G.txt" --RequiredLengthGiB 1
@GenerateFile --FilePath "d:\DataSort\DataSort-10G.txt" --RequiredLengthGiB 10
@GenerateFile --FilePath "d:\DataSort\DataSort-14G.txt" --RequiredLengthGiB 14

@GenerateFile --FilePath "d:\DataSort\DataSort-50G.txt" --RequiredLengthGiB 50
@GenerateFile --FilePath "d:\DataSort\DataSort-100G.txt" --RequiredLengthGiB 100

@Cd ".\..\..\..\..\"