FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

COPY ["src/MediaProcessing.Shared/MediaProcessing.Shared.csproj", "src/MediaProcessing.Shared/"]
COPY ["src/MediaWorker/MediaWorker.csproj", "src/MediaWorker/"]
RUN dotnet restore "src/MediaWorker/MediaWorker.csproj"

COPY src/MediaProcessing.Shared/ src/MediaProcessing.Shared/
COPY src/MediaWorker/ src/MediaWorker/
WORKDIR "/src/src/MediaWorker"
RUN dotnet publish "MediaWorker.csproj" -c Release -o /app/publish /p:UseAppHost=false

FROM mcr.microsoft.com/dotnet/runtime:10.0 AS final
WORKDIR /app
COPY --from=build /app/publish .

ENTRYPOINT ["dotnet", "MediaWorker.dll"]
