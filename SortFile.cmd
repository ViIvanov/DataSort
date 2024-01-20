@Cd "SortFile\bin\Release\net8.0"
@Rem @SortFile --FilePath ".\..\..\..\..\Results\DataSort-1G.txt"  --MaxReadLines 5000
@SortFile --FilePath ".\..\..\..\..\Results\DataSort-10G.txt"  --MaxReadLines 10000
@Rem @SortFile --FilePath "d:\DataSort\DataSort-1G.txt"  --MaxReadLines 50000
@Rem @SortFile --FilePath "d:\DataSort\DataSort-10G.txt"  --MaxReadLines 50000
@Rem @SortFile --FilePath "d:\DataSort\DataSort-14G.txt"  --MaxReadLines 50000

@Rem @SortFile --FilePath "d:\DataSort\HugeFileSort-10M.txt"  --MaxReadLines 200000
@Rem @SortFile --FilePath "d:\DataSort\HugeFileSort-100M.txt"  --MaxReadLines 400000
@Rem @SortFile --FilePath ".\..\..\..\..\Results\HugeFileSort-100M.txt"  --MaxReadLines 400000

@Cd ".\..\..\..\..\"