FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

COPY ["src/MediaProcessing.Shared/MediaProcessing.Shared.csproj", "src/MediaProcessing.Shared/"]
COPY ["src/MediaClient.Console/MediaClient.Console.csproj", "src/MediaClient.Console/"]
RUN dotnet restore "src/MediaClient.Console/MediaClient.Console.csproj"

COPY src/MediaProcessing.Shared/ src/MediaProcessing.Shared/
COPY src/MediaClient.Console/ src/MediaClient.Console/
WORKDIR "/src/src/MediaClient.Console"
RUN dotnet publish "MediaClient.Console.csproj" -c Release -o /app/publish /p:UseAppHost=false

FROM mcr.microsoft.com/dotnet/runtime:10.0 AS final
WORKDIR /app
COPY --from=build /app/publish .

ENTRYPOINT ["dotnet", "MediaClient.Console.dll"]
