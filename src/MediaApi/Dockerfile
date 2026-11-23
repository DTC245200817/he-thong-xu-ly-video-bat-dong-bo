FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

COPY ["src/MediaProcessing.Shared/MediaProcessing.Shared.csproj", "src/MediaProcessing.Shared/"]
COPY ["src/MediaApi/MediaApi.csproj", "src/MediaApi/"]
RUN dotnet restore "src/MediaApi/MediaApi.csproj"

COPY src/MediaProcessing.Shared/ src/MediaProcessing.Shared/
COPY src/MediaApi/ src/MediaApi/
WORKDIR "/src/src/MediaApi"
RUN dotnet publish "MediaApi.csproj" -c Release -o /app/publish /p:UseAppHost=false

FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS final
WORKDIR /app
COPY --from=build /app/publish .

ENV ASPNETCORE_URLS=http://+:8080
EXPOSE 8080

ENTRYPOINT ["dotnet", "MediaApi.dll"]
